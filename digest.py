import os
import logging
import hashlib
import json
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
from datetime import datetime
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
CACHE_PATH = Path(".cache") / "seen_urls.json"
STALENESS_DAYS = 3


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


def is_stale(url, seen):
    key = hashlib.md5(url.encode()).hexdigest()
    if key not in seen:
        return False
    last_seen = datetime.fromisoformat(seen[key])
    return (datetime.now() - last_seen).days < STALENESS_DAYS


def mark_seen(url, seen):
    key = hashlib.md5(url.encode()).hexdigest()
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


CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}


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
# 2a. Fetch RSS (Google News)
# ----------------------------
def fetch_rss(name):
    logger.info("Fetching RSS for: %s", name)
    query = quote_plus(f"{name} music tour interview announcement OR release")
    url = f"https://news.google.com/rss/search?q={query}&hl=en-GB&gl=GB&ceid=GB:en"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logger.warning("RSS fetch failed for %s: %s", name, e)
        return []
    root = ET.fromstring(response.content)
    items = []
    for item in root.findall(".//item")[:5]:
        title = item.find("title").text if item.find("title") is not None else ""
        link = item.find("link").text if item.find("link") is not None else ""
        pub_date = item.find("pubDate").text if item.find("pubDate") is not None else ""
        items.append({"title": title, "link": link, "date": pub_date, "source_type": "rss"})
    logger.info("RSS found %d items for %s", len(items), name)
    return items


# ----------------------------
# 2b. Fetch Web Link content
# ----------------------------
def fetch_web_link(url):
    if not url:
        return []
    logger.info("Fetching web link: %s", url)

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
def fetch_updates(interest, seen_urls):
    name = interest["name"]
    web = interest.get("web")

    rss_items = fetch_rss(name)
    web_items = fetch_web_link(web) if web else []

    all_items = rss_items + web_items

    fresh_items = []
    stale_count = 0
    for item in all_items:
        if item["link"] and is_stale(item["link"], seen_urls):
            stale_count += 1
        else:
            fresh_items.append(item)
            if item["link"]:
                mark_seen(item["link"], seen_urls)

    if stale_count:
        logger.info("Filtered %d stale items for %s", stale_count, name)

    logger.info(
        "Total fresh items for %s: %d (rss=%d, web=%d)",
        name, len(fresh_items), len(rss_items), len(web_items)
    )
    return fresh_items


# ----------------------------
# 3. AI summariser — structured output
# ----------------------------
def summarise(all_updates, interests):
    logger.info("Generating structured summary via OpenAI")

    interest_names = ", ".join(i["name"] for i in interests)

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

Rules:
- important_alerts: HIGH-CONFIDENCE events only (releases, tours, official announcements, collaborations)
- summary_bullets: one bullet per significant finding
- skip_list: repeated, low-relevance, or off-topic items
- sources_used: all source links or titles referenced
- Only use provided data — do NOT invent anything
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

    today = datetime.now().strftime("%Y-%m-%d")
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

    # Plain-text brief for logs / future notification workflows
    brief_lines = [f"=== DAILY BRIEF {today} ===", summary_text]
    if bullets:
        brief_lines += bullets
    if alerts:
        brief_lines.append("ALERTS:")
        brief_lines += [f"  - {a['name']}: {a['update']}" for a in alerts]
    logger.info("\n%s", "\n".join(brief_lines))

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

    all_updates = ""
    items_by_interest = {}

    for interest in interests:
        items = fetch_updates(interest, seen_urls)
        items_by_interest[interest["name"]] = items

        all_updates += f"\n\n{interest['name']}:\n"
        if not items:
            all_updates += "No recent news found\n"
            continue
        for u in items:
            all_updates += f"- {u['title']}\n  {u.get('date', '')}\n  {u['link']}\n"

    logger.info("\n%s\n%s\n%s", "=" * 60, all_updates.strip(), "=" * 60)

    raw = summarise(all_updates, interests)
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