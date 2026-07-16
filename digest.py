import os
import logging
import hashlib
import json
import re
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus, urlsplit, urlunsplit, parse_qsl, urlencode
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from notion_client import Client
from openai import OpenAI
from dotenv import load_dotenv

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ----------------------------
# Env
# ----------------------------
load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

INTERESTS_DB_ID = os.getenv("INTERESTS_DB_ID")
DIGEST_DB_ID = os.getenv("DIGEST_DB_ID")
DISCOVERY_DB_ID = os.getenv("DISCOVERY_DB_ID")

notion = Client(auth=NOTION_TOKEN)
ai = OpenAI(api_key=OPENAI_API_KEY)

# ----------------------------
# Staleness cache (3-day novelty window)
# ----------------------------
CACHE_PATH = Path(__file__).parent / ".cache" / "seen_urls.json"
STALENESS_DAYS = 7
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "msclkid", "twclid", "mc_cid", "mc_eid", "ref", "ref_src"
}
NEWS_QUERY_TERMS = [
    "news",
    "announcement",
    "release",
    "tour",
    "live show",
    "festival",
    "interview",
    "podcast",
    "tv appearance",
    "radio appearance",
    "talk",
    "speech",
    "panel",
    "gallery show",
    "art exhibition",
    "collaboration"
]


def load_seen_urls():
    if not CACHE_PATH.exists():
        return {}
    try:
        with open(CACHE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_seen_urls(seen):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(seen, f)


def canonicalize_url(url):
    """Normalize URL to reduce duplicate variants caused by trackers and fragments."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        scheme = parts.scheme.lower() if parts.scheme else "https"
        netloc = parts.netloc.lower()
        path = parts.path or ""
        if path != "/":
            path = path.rstrip("/")

        params = []
        for key, value in parse_qsl(parts.query, keep_blank_values=False):
            key_lower = key.lower()
            if key_lower.startswith(TRACKING_QUERY_PREFIXES) or key_lower in TRACKING_QUERY_KEYS:
                continue
            params.append((key, value))
        params.sort()

        normalized_query = urlencode(params)
        return urlunsplit((scheme, netloc, path, normalized_query, ""))
    except Exception:
        return url.strip()


def normalize_title_for_fingerprint(title):
    """Create a stable title string for same-story dedupe across different links."""
    cleaned = (title or "").lower().strip()
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def item_dedupe_keys(item, interest_name):
    """Return cache keys used for duplicate detection (url + title fingerprint)."""
    keys = []
    canonical = canonicalize_url(item.get("link", ""))
    if canonical:
        keys.append(f"url:{hashlib.md5(canonical.encode()).hexdigest()}")

    title_norm = normalize_title_for_fingerprint(item.get("title", ""))
    if title_norm:
        title_seed = f"{interest_name.lower()}|{title_norm}"
        keys.append(f"title:{hashlib.md5(title_seed.encode()).hexdigest()}")
    return keys


def is_stale_key(key, seen):
    if key not in seen:
        return False
    last_seen = datetime.fromisoformat(seen[key])
    return (datetime.now() - last_seen).days < STALENESS_DAYS


def mark_seen_key(key, seen):
    seen[key] = datetime.now().isoformat()


# ----------------------------
# Phase 1: Parse / validate helpers
# ----------------------------
def parse_json_safe(text, fallback):
    """Parse JSON from AI response, stripping markdown fences, with safe fallback."""
    try:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            parts = cleaned.split("```")
            cleaned = parts[1] if len(parts) > 1 else cleaned
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
        return json.loads(cleaned.strip())
    except Exception as e:
        logger.warning("JSON parse failed: %s — using fallback", e)
        return fallback


def normalize_name(name):
    """Normalize artist name for dedup comparison."""
    return " ".join(name.strip().lower().split())


def notion_page_url(page_id):
    """Return direct Notion URL for a page id."""
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def update_full_report_property(digest_page_id, digest_page_url):
    """Populate the Full Report property using the database's configured property type."""
    try:
        db = notion.databases.retrieve(database_id=DIGEST_DB_ID)
        props = db.get("properties", {})
        full_report_prop = props.get("Full Report")

        if not full_report_prop:
            logger.warning("Digest DB has no 'Full Report' property; skipping link update")
            return

        prop_type = full_report_prop.get("type")
        logger.info("Full Report property type: %s", prop_type)

        if prop_type == "url":
            notion.pages.update(
                page_id=digest_page_id,
                properties={"Full Report": {"url": digest_page_url}}
            )
            logger.info("Full Report URL property updated")
            return

        if prop_type == "rich_text":
            notion.pages.update(
                page_id=digest_page_id,
                properties={
                    "Full Report": {
                        "rich_text": [{"text": {"content": digest_page_url}}]
                    }
                }
            )
            logger.info("Full Report rich_text property updated")
            return

        if prop_type == "relation":
            relation_cfg = full_report_prop.get("relation", {})
            target_db_id = relation_cfg.get("database_id")

            # If relation points to this same DB, self-link the current digest row.
            if target_db_id == DIGEST_DB_ID:
                notion.pages.update(
                    page_id=digest_page_id,
                    properties={"Full Report": {"relation": [{"id": digest_page_id}]}}
                )
                logger.info("Full Report relation property updated with self-link")
                return

            logger.warning(
                "Full Report relation targets another DB (%s); cannot auto-link without creating a row there",
                target_db_id
            )
            return

        logger.warning("Unsupported Full Report property type '%s'; skipping update", prop_type)
    except Exception as e:
        logger.warning("Failed to update Full Report property: %s", e)


CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}
RECENT_HISTORY_DAYS = 7
RECENT_HISTORY_MAX_PAGES = 6


# ----------------------------
# 0. Recent digest history (avoid re-reporting the same story)
# ----------------------------
def get_recent_digest_history(days=RECENT_HISTORY_DAYS, max_pages=RECENT_HISTORY_MAX_PAGES):
    """Pull the Summary text of recent digest rows so the AI can see what's already
    been reported and avoid treating the same ongoing story as new every day."""
    logger.info("Fetching last %d days of digest history for continuity context", days)
    try:
        results = notion.databases.query(
            database_id=DIGEST_DB_ID,
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=max_pages
        )
    except Exception as e:
        logger.warning("Could not fetch digest history: %s", e)
        return []

    cutoff = datetime.now() - timedelta(days=days)
    history = []
    for page in results.get("results", []):
        props = page.get("properties", {})
        title_parts = props.get("Date", {}).get("title", [])
        date_str = title_parts[0]["text"]["content"] if title_parts else ""
        try:
            page_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        except Exception:
            continue
        if page_date < cutoff:
            continue

        summary_parts = props.get("Summary", {}).get("rich_text", [])
        summary_text = summary_parts[0]["text"]["content"] if summary_parts else ""
        if summary_text:
            history.append({"date": date_str, "summary": summary_text})

    logger.info("Found %d prior digest(s) within the last %d days", len(history), days)
    return history


def format_recent_history(history):
    """Render prior digest summaries as a block of text for the AI prompt."""
    if not history:
        return "(no prior digests in the lookback window — treat everything as potentially new)"
    lines = []
    for entry in history:
        lines.append(f"- [{entry['date']}] {entry['summary']}")
    return "\n".join(lines)


# ----------------------------
# 1. Get Interests
# ----------------------------
def get_interests():
    logger.info("Fetching interests from Notion")
    results = notion.databases.query(database_id=INTERESTS_DB_ID)
    interests = []
    for r in results["results"]:
        props = r["properties"]
        name = props["Name"]["title"][0]["text"]["content"] if props["Name"]["title"] else ""
        web = props["Web Link"]["url"]
        ig = props["Instagram"]["url"]
        interests.append({"name": name, "web": web, "instagram": ig})
    logger.info("Found %d interests", len(interests))
    return interests


# ----------------------------
# 2a. Fetch RSS (Google News + Bing News)
# ----------------------------
def _parse_rss_items(content, max_items=5, source_type="rss"):
    """Parse raw RSS/XML bytes into item dicts."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logger.warning("XML parse error: %s", e)
        return []
    items = []
    for item in root.findall(".//item")[:max_items]:
        title = item.find("title").text if item.find("title") is not None else ""
        link = item.find("link").text if item.find("link") is not None else ""
        pub_date = item.find("pubDate").text if item.find("pubDate") is not None else ""
        items.append({"title": title, "link": link, "date": pub_date, "source_type": source_type})
    return items


def _fetch_rss_url(url, source_type="rss", name="", max_items=6):
    """Fetch a feed URL and parse it, preferring feedparser (handles both RSS 2.0 and Atom —
    e.g. YouTube/Reddit feeds are Atom and have no <item> tags, only <entry>)."""
    if HAS_FEEDPARSER:
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
            if feed.entries:
                items = []
                for entry in feed.entries[:max_items]:
                    items.append({
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "date": entry.get("published", entry.get("updated", "")),
                        "source_type": source_type
                    })
                return items
        except Exception as e:
            logger.warning("feedparser failed (%s) for %s: %s", source_type, name, e)

    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        return _parse_rss_items(response.content, max_items=max_items, source_type=source_type)
    except Exception as e:
        logger.warning("RSS fetch failed (%s) for %s: %s", source_type, name, e)
        return []


def _merge_new(items, new_items):
    """Append items from new_items whose link isn't already present in items."""
    existing_links = {i["link"] for i in items if i["link"]}
    items += [i for i in new_items if i["link"] not in existing_links]
    return items


def fetch_rss(name):
    logger.info("Fetching RSS for: %s", name)
    items = []

    # 1. Google News — targeted event/news query
    terms = " OR ".join(f'"{term}"' if " " in term else term for term in NEWS_QUERY_TERMS)
    query_text = f'"{name}" ({terms})'
    gn_url = f"https://news.google.com/rss/search?q={quote_plus(query_text)}&hl=en-GB&gl=GB&ceid=GB:en"
    items += _fetch_rss_url(gn_url, source_type="rss_google_news", name=name)

    # 2. Google News — broader query (catches reviews, profiles, general press)
    gn_broad_query = '"' + name + '"'
    gn_broad_url = f"https://news.google.com/rss/search?q={quote_plus(gn_broad_query)}&hl=en-GB&gl=GB&ceid=GB:en"
    _merge_new(items, _fetch_rss_url(gn_broad_url, source_type="rss_google_broad", name=name))

    # 3. Bing News RSS
    bing_url = f"https://www.bing.com/news/search?q={quote_plus(f'{name} music art')}&format=rss"
    _merge_new(items, _fetch_rss_url(bing_url, source_type="rss_bing", name=name))

    # 4. GDELT — global news index, independent of Google/Bing's own ranking/dedup logic
    gdelt_url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(name)}&mode=artlist&maxrecords=8&format=RSS&sort=datedesc"
    )
    _merge_new(items, _fetch_rss_url(gdelt_url, source_type="rss_gdelt", name=name))

    # 5. Reddit search — catches fan/community chatter often ahead of press coverage
    reddit_url = f"https://www.reddit.com/search.rss?q={quote_plus(name)}&sort=new&limit=8"
    _merge_new(items, _fetch_rss_url(reddit_url, source_type="rss_reddit", name=name))

    logger.info("RSS found %d items for %s (google x2 + bing + gdelt + reddit)", len(items), name)
    return items


# ----------------------------
# 2b. Fetch Web Link content
# ----------------------------
_YT_CHANNEL_RE = re.compile(
    r"youtube\.com/(?:channel/(UC[\w-]+)|@([\w.-]+)|c/([\w.-]+)|user/([\w.-]+))"
)


def _youtube_channel_rss(url):
    """Return RSS items from a YouTube channel page URL, or [] if not a YouTube URL."""
    m = _YT_CHANNEL_RE.search(url)
    if not m:
        return []
    channel_id, handle, legacy_c, legacy_user = m.groups()
    if channel_id:
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        logger.info("Fetching YouTube channel RSS: %s", feed_url)
        return _fetch_rss_url(feed_url, source_type="youtube_rss")

    # For handle/custom/user URLs we need to resolve the channel ID first
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        cid_match = re.search(r'"channelId"\s*:\s*"(UC[\w-]+)"', resp.text)
        if cid_match:
            feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={cid_match.group(1)}"
            logger.info("Resolved YouTube channel RSS: %s", feed_url)
            return _fetch_rss_url(feed_url, source_type="youtube_rss")
    except Exception as e:
        logger.warning("YouTube channel ID resolution failed for %s: %s", url, e)
    return []


def fetch_web_link(url):
    if not url:
        return []
    logger.info("Fetching web link: %s", url)

    # YouTube channel — grab video RSS feed directly
    yt_items = _youtube_channel_rss(url)
    if yt_items:
        return yt_items

    # Try feedparser first (handles feeds and some HTML pages)
    if HAS_FEEDPARSER:
        try:
            feed = feedparser.parse(url)
            if feed.entries:
                items = []
                for entry in feed.entries[:5]:
                    items.append({
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "date": entry.get("published", ""),
                        "source_type": "web_feed"
                    })
                logger.info("feedparser found %d items from %s", len(items), url)
                return items
        except Exception as e:
            logger.warning("feedparser failed for %s: %s", url, e)

    # Fallback: HTML metadata + headline extraction
    if HAS_BS4:
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            items = []

            og_title = soup.find("meta", property="og:title")
            og_desc = soup.find("meta", property="og:description")
            title = (og_title.get("content") if og_title else None) or (soup.title.text.strip() if soup.title else "")
            desc = og_desc.get("content") if og_desc else ""
            if title:
                combined = f"{title}. {desc}".strip(" .")
                items.append({"title": combined, "link": url, "date": "", "source_type": "web_meta"})

            for tag in (soup.find_all("h1")[:2] + soup.find_all("h2")[:3]):
                text = tag.get_text(strip=True)
                if text and len(text) > 10:
                    items.append({"title": text, "link": url, "date": "", "source_type": "web_headline"})

            logger.info("HTML extraction found %d items from %s", len(items), url)
            return items[:5]
        except Exception as e:
            logger.warning("HTML fetch failed for %s: %s", url, e)

    return []


# ----------------------------
# 2c. Merged fetch with staleness filter
# ----------------------------
MAX_ITEM_AGE_DAYS = 5


def _parse_item_date(date_str):
    """Parse an RFC-822/ISO pub date string into an aware datetime, or None if unparseable."""
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None


def _is_recent_item(item, max_age_days=MAX_ITEM_AGE_DAYS):
    """Keep items published within the window, or items with no parseable date
    (e.g. scraped web metadata) so we don't silently drop a whole source."""
    dt = _parse_item_date(item.get("date", ""))
    if dt is None:
        return True
    age_days = (datetime.now(timezone.utc) - dt).days
    return age_days <= max_age_days


def fetch_updates(interest, seen_urls):
    name = interest["name"]
    web = interest.get("web")

    rss_items = fetch_rss(name)
    web_items = fetch_web_link(web) if web else []

    all_items = rss_items + web_items

    # Google/Bing News often rank by relevance, not recency, so the same "evergreen"
    # articles surface every run. Sort newest-first and drop anything stale so the
    # per-source cap keeps the freshest items instead of the most "relevant" ones.
    all_items.sort(key=lambda i: _parse_item_date(i.get("date", "")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    recent_count_before = len(all_items)
    all_items = [i for i in all_items if _is_recent_item(i)]
    dropped_old = recent_count_before - len(all_items)
    if dropped_old:
        logger.info("Dropped %d stale (>%dd old) items for %s", dropped_old, MAX_ITEM_AGE_DAYS, name)

    fresh_items = []
    stale_url_count = 0
    stale_title_count = 0
    in_run_duplicate_count = 0
    run_seen_keys = set()

    for item in all_items:
        keys = item_dedupe_keys(item, name)
        if not keys:
            fresh_items.append(item)
            continue

        if any(key in run_seen_keys for key in keys):
            in_run_duplicate_count += 1
            continue

        stale_hit = False
        for key in keys:
            if is_stale_key(key, seen_urls):
                stale_hit = True
                if key.startswith("url:"):
                    stale_url_count += 1
                elif key.startswith("title:"):
                    stale_title_count += 1
                break

        if stale_hit:
            continue

        fresh_items.append(item)
        for key in keys:
            run_seen_keys.add(key)
            mark_seen_key(key, seen_urls)

    if stale_url_count or stale_title_count or in_run_duplicate_count:
        logger.info(
            "Filtered duplicates for %s: stale_url=%d stale_title=%d in_run=%d",
            name, stale_url_count, stale_title_count, in_run_duplicate_count
        )

    logger.info(
        "Total fresh items for %s: %d (rss=%d, web=%d, raw=%d)",
        name, len(fresh_items), len(rss_items), len(web_items), len(all_items)
    )
    return fresh_items


# ----------------------------
# 3. AI summariser — structured output
# ----------------------------
def summarise(all_updates, interests, recent_history=None):
    logger.info("Generating structured summary via OpenAI")

    interest_names = ", ".join(i["name"] for i in interests)
    history_block = format_recent_history(recent_history or [])

    prompt = f"""
You are a CULTURE INTELLIGENCE assistant analysing updates for: {interest_names}

Return STRICT JSON — no markdown fences, no extra text:

{{
  "summary": "1-2 sentence overall digest",
  "summary_bullets": ["bullet 1", "bullet 2"],
  "important_alerts": [
    {{
      "name": "artist name",
      "update": "what happened",
      "relevance": "London | Global | None"
    }}
  ],
  "why_it_matters": "1-2 sentences on cultural significance",
  "quick_actions": ["action 1"],
  "skip_list": ["noise item 1"],
  "sources_used": ["url or headline title"]
}}

PREVIOUSLY REPORTED (last {RECENT_HISTORY_DAYS} days — do NOT re-report these as new):
{history_block}

Rules:
- Compare today's DATA against PREVIOUSLY REPORTED above. If an item is just continued
  coverage of a story already reported (same release, same tour, same announcement) with
  no materially new development, put it in skip_list, not summary_bullets/important_alerts.
- Only put something in important_alerts or summary_bullets if it is either a genuinely new
  story, or a concrete NEW development on a known story (e.g. a date/venue/detail confirmed
  that wasn't known before). Restating the same fact in different words is NOT new.
- important_alerts: HIGH-CONFIDENCE events only (releases, tours, official announcements, collaborations)
- summary_bullets: one bullet per significant NEW finding
- skip_list: repeated/already-reported, low-relevance, or off-topic items
- sources_used: all source links or titles referenced
- Only use provided data — do NOT invent anything
- It is completely normal and expected for there to be NO new news for an interest on a
  given day. Do not manufacture significance to fill space. If, after filtering out
  already-reported stories, nothing new remains for an interest, say so plainly (e.g.
  "No new developments for X today") rather than re-surfacing old coverage.
- If nothing exists return empty lists []

DATA:
{all_updates}
"""

    response = ai.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


# ----------------------------
# 4. Extract discoveries
# ----------------------------
def extract_discoveries(text):
    logger.info("Extracting discoveries")

    prompt = f"""
From this digest, extract NEW people/artists mentioned
that are NOT the main subjects.

Return strict JSON list — no markdown fences:
[
  {{
    "name": "",
    "reason": "",
    "source": "",
    "confidence": "low|medium|high"
  }}
]

TEXT:
{text}
"""

    response = ai.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


# ----------------------------
# 5. Write digest + full report page
# ----------------------------
def write_digest(summary_data, items_by_interest):
    logger.info("Writing digest to Notion")
    logger.info("Target Digest DB: %s", DIGEST_DB_ID)

    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    summary_text = summary_data.get("summary", "")
    alerts = summary_data.get("important_alerts", [])
    bullets = summary_data.get("summary_bullets", [])
    why = summary_data.get("why_it_matters", "")
    quick_actions = summary_data.get("quick_actions", [])
    skip_list = summary_data.get("skip_list", [])

    logger.info("Important alerts count: %d", len(alerts))

    # Create the digest row (also serves as the full report page)
    page = notion.pages.create(
        parent={"database_id": DIGEST_DB_ID},
        properties={
            "Date": {"title": [{"text": {"content": today}}]},
            "Important Alerts": {"number": len(alerts)},
            "Summary": {"rich_text": [{"text": {"content": summary_text[:2000]}}]}
        }
    )
    digest_page_id = page["id"]
    digest_page_url = notion_page_url(digest_page_id)
    logger.info("Digest page created: %s", digest_page_id)
    logger.info("Digest page URL: %s", digest_page_url)

    # Build full report blocks
    blocks = []

    blocks.append({
        "object": "block", "type": "heading_1",
        "heading_1": {"rich_text": [{"type": "text", "text": {"content": f"Daily Digest — {today}"}}]}
    })

    if bullets:
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Summary"}}]}
        })
        for b in bullets:
            blocks.append({
                "object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": b}}]}
            })

    if why:
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Why It Matters Today"}}]}
        })
        blocks.append({
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": why}}]}
        })

    if quick_actions:
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Quick Actions"}}]}
        })
        for action in quick_actions:
            blocks.append({
                "object": "block", "type": "to_do",
                "to_do": {"rich_text": [{"type": "text", "text": {"content": action}}], "checked": False}
            })

    if alerts:
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Important Alerts"}}]}
        })
        for alert in alerts:
            content = f"[{alert.get('relevance', '')}] {alert.get('name', '')}: {alert.get('update', '')}"
            blocks.append({
                "object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": content}}]}
            })

    if items_by_interest:
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Sources"}}]}
        })
        for interest_name, items in items_by_interest.items():
            blocks.append({
                "object": "block", "type": "heading_3",
                "heading_3": {"rich_text": [{"type": "text", "text": {"content": interest_name}}]}
            })
            if not items:
                blocks.append({
                    "object": "block", "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": f"No new news regarding {interest_name}."}
                        }]
                    }
                })
                continue
            for item in items:
                label = item["title"] or item["link"]
                if item["link"]:
                    blocks.append({
                        "object": "block", "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{"type": "text", "text": {"content": label, "link": {"url": item["link"]}}}]
                        }
                    })
                else:
                    blocks.append({
                        "object": "block", "type": "bulleted_list_item",
                        "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": label}}]}
                    })

    if skip_list:
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Skip List (Noise Filtered)"}}]}
        })
        for s in skip_list:
            blocks.append({
                "object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": s}}]}
            })

    # Append blocks to digest page in batches of 100 (Notion API limit)
    BLOCK_LIMIT = 100
    for i in range(0, len(blocks), BLOCK_LIMIT):
        notion.blocks.children.append(
            block_id=digest_page_id,
            children=blocks[i:i + BLOCK_LIMIT]
        )
    logger.info("Full report appended to digest page %s", digest_page_id)

    # Read back page metadata to make visibility/debugging issues obvious in logs.
    try:
        page_check = notion.pages.retrieve(page_id=digest_page_id)
        parent = page_check.get("parent", {})
        logger.info(
            "Digest page verify: archived=%s, in_trash=%s, parent=%s",
            page_check.get("archived", False),
            page_check.get("in_trash", False),
            parent
        )
    except Exception as e:
        logger.warning("Digest page verify failed for %s: %s", digest_page_id, e)

    # Plain-text brief for logs / future notification workflows
    brief_lines = [f"=== DAILY BRIEF {today} ===", summary_text]
    if bullets:
        brief_lines += bullets
    if alerts:
        brief_lines.append("ALERTS:")
        brief_lines += [f"  - {a['name']}: {a['update']}" for a in alerts]
    logger.info("\n%s", "\n".join(brief_lines))

    # Ensure the digest row contains a clickable pointer in the Full Report column.
    update_full_report_property(digest_page_id, digest_page_url)
    logger.info("Open digest page directly: %s", digest_page_url)

    return digest_page_id


# ----------------------------
# 6. Write discoveries (dedup + watchlist semantics)
# ----------------------------
def write_discoveries(discoveries_json):
    logger.info("Writing discoveries")

    items = parse_json_safe(discoveries_json, fallback=[])
    if not items:
        logger.info("No discoveries to write")
        return

    # Load existing discoveries for dedup
    existing = {}
    try:
        results = notion.databases.query(database_id=DISCOVERY_DB_ID)
        for page in results["results"]:
            props = page["properties"]
            title_parts = props.get("Artist Name", {}).get("title", [])
            name = title_parts[0]["text"]["content"] if title_parts else ""
            norm = normalize_name(name)
            watchlisted = props.get("Added to Watchlist", {}).get("checkbox", False)
            existing[norm] = {"page_id": page["id"], "watchlisted": watchlisted}
    except Exception as e:
        logger.warning("Could not load existing discoveries: %s", e)

    created = 0
    updated = 0
    watchlisted_rediscoveries = 0

    for item in items:
        norm = normalize_name(item.get("name", ""))
        if not norm:
            continue

        confidence = item.get("confidence", "low")
        if confidence not in CONFIDENCE_RANK:
            confidence = "low"

        if norm in existing:
            entry = existing[norm]
            page_id = entry["page_id"]
            watchlisted = entry["watchlisted"]
            if watchlisted:
                watchlisted_rediscoveries += 1

            try:
                current = notion.pages.retrieve(page_id=page_id)
                curr_props = current["properties"]

                reason_parts = curr_props.get("Why Recommended", {}).get("rich_text", [])
                curr_reason = reason_parts[0]["text"]["content"] if reason_parts else ""

                source_parts = curr_props.get("Source", {}).get("rich_text", [])
                curr_source = source_parts[0]["text"]["content"] if source_parts else ""

                curr_conf_obj = curr_props.get("Confidence", {}).get("select")
                curr_conf = curr_conf_obj["name"] if curr_conf_obj else "low"
                if curr_conf not in CONFIDENCE_RANK:
                    curr_conf = "low"

                # Append context; only upgrade confidence, never downgrade
                new_reason = f"{curr_reason}\n[Update] {item.get('reason', '')}".strip()[:2000]
                new_source = f"{curr_source}\n{item.get('source', '')}".strip()[:2000]
                merged_conf = confidence if CONFIDENCE_RANK[confidence] > CONFIDENCE_RANK[curr_conf] else curr_conf

                notion.pages.update(
                    page_id=page_id,
                    properties={
                        "Why Recommended": {"rich_text": [{"text": {"content": new_reason}}]},
                        "Source": {"rich_text": [{"text": {"content": new_source}}]},
                        "Confidence": {"select": {"name": merged_conf}},
                        "Added to Watchlist": {"checkbox": watchlisted}  # always preserve
                    }
                )
                updated += 1
                logger.info(
                    "Updated discovery: %s (watchlisted=%s, confidence=%s)",
                    item["name"], watchlisted, merged_conf
                )
            except Exception as e:
                logger.warning("Failed to update discovery %s: %s", item["name"], e)
        else:
            try:
                notion.pages.create(
                    parent={"database_id": DISCOVERY_DB_ID},
                    properties={
                        "Artist Name": {"title": [{"text": {"content": item.get("name", "")}}]},
                        "Why Recommended": {"rich_text": [{"text": {"content": item.get("reason", "")[:2000]}}]},
                        "Source": {"rich_text": [{"text": {"content": item.get("source", "")[:2000]}}]},
                        "Confidence": {"select": {"name": confidence}},
                        "Added to Watchlist": {"checkbox": False}
                    }
                )
                created += 1
                logger.info("Created discovery: %s (confidence=%s)", item["name"], confidence)
            except Exception as e:
                logger.warning("Failed to create discovery %s: %s", item["name"], e)

    logger.info(
        "Discoveries: created=%d, updated=%d, watchlisted_rediscoveries=%d",
        created, updated, watchlisted_rediscoveries
    )


# ----------------------------
# MAIN PIPELINE
# ----------------------------
def run():
    logger.info("Starting pipeline")

    interests = get_interests()
    seen_urls = load_seen_urls()
    recent_history = get_recent_digest_history()

    all_updates = ""
    items_by_interest = {}

    for interest in interests:
        items = fetch_updates(interest, seen_urls)
        items_by_interest[interest["name"]] = items

        all_updates += f"\n\n{interest['name']}:\n"
        if not items:
            all_updates += f"No new news regarding {interest['name']}.\n"
            continue
        for u in items:
            all_updates += f"- {u['title']}\n  {u.get('date', '')}\n  {u['link']}\n"

    logger.info("\n%s\n%s\n%s", "=" * 60, all_updates.strip(), "=" * 60)

    raw = summarise(all_updates, interests, recent_history=recent_history)
    logger.info("\n%s\nOPENAI RAW RESPONSE:\n%s\n%s", "-" * 60, raw.strip(), "-" * 60)

    summary_data = parse_json_safe(raw, fallback={
        "summary": raw,
        "summary_bullets": [],
        "important_alerts": [],
        "why_it_matters": "",
        "quick_actions": [],
        "skip_list": [],
        "sources_used": []
    })

    write_digest(summary_data, items_by_interest)

    discoveries_raw = extract_discoveries(summary_data.get("summary", raw))
    write_discoveries(discoveries_raw)

    save_seen_urls(seen_urls)

    logger.info("Pipeline complete")


if __name__ == "__main__":
    run()