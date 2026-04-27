import os
import logging
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
from notion_client import Client
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
import json

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

        interests.append({
            "name": name,
            "web": web,
            "instagram": ig
        })

    logger.info("Found %d interests", len(interests))
    return interests


# ----------------------------
# 2. Fetch RSS updates
# ----------------------------
def fetch_updates(name):
    logger.info("Fetching RSS updates for: %s", name)

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

        items.append({
            "title": title,
            "link": link,
            "date": pub_date
        })

    logger.info("Found %d RSS items for %s", len(items), name)
    return items


# ----------------------------
# 3. AI summariser (STRUCTURED OUTPUT)
# ----------------------------
def summarise(all_updates):
    logger.info("Generating structured summary via OpenAI")

    prompt = f"""
You are a CULTURE INTELLIGENCE assistant.

You must extract ONLY real cultural updates.

Return STRICT JSON in this format:

{{
  "summary": "concise cultural digest",
  "important_alerts": [
    {{
      "name": "artist name",
      "update": "what happened",
      "relevance": "London | Global | None"
    }}
  ]
}}

Rules:
- Only use provided data
- Do NOT invent anything
- If nothing exists, return empty list []
- Focus only on:
  tours, releases, collaborations, interviews

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

Return JSON list:
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
# 5. Write digest to Notion
# ----------------------------
def write_digest(summary, alerts):
    logger.info("Writing digest to Notion")

    today = datetime.now().strftime("%Y-%m-%d")

    alerts_count = len(alerts)

    logger.info("Important alerts count: %d", alerts_count)

    notion.pages.create(
        parent={"database_id": DIGEST_DB_ID},
        properties={
            "Date": {"title": [{"text": {"content": today}}]},
            "Important Alerts": {"number": alerts_count},
            "Summary": {"rich_text": [{"text": {"content": summary}}]}
        }
    )


# ----------------------------
# 6. Write discoveries
# ----------------------------
def write_discoveries(discoveries_json):
    logger.info("Writing discoveries")

    try:
        items = json.loads(discoveries_json)
    except Exception:
        logger.warning("Failed to parse discoveries JSON")
        return

    logger.info("Writing %d discoveries", len(items))

    for item in items:
        notion.pages.create(
            parent={"database_id": DISCOVERY_DB_ID},
            properties={
                "Artist Name": {"title": [{"text": {"content": item["name"]}}]},
                "Why Recommended": {"rich_text": [{"text": {"content": item["reason"]}}]},
                "Source": {"rich_text": [{"text": {"content": item["source"]}}]},
                "Confidence": {"select": {"name": item["confidence"]}},
                "Added to Watchlist": {"checkbox": False}
            }
        )


# ----------------------------
# MAIN PIPELINE
# ----------------------------
def run():
    logger.info("Starting pipeline")

    interests = get_interests()

    all_updates = ""

    for i in interests:
        updates = fetch_updates(i["name"])

        all_updates += f"\n\n{i['name']}:\n"

        if not updates:
            all_updates += "No recent news found\n"
            continue

        for u in updates:
            all_updates += f"- {u['title']}\n  {u['date']}\n  {u['link']}\n"

    logger.info("\n%s\n%s\n%s", "=" * 60, all_updates.strip(), "=" * 60)

    raw = summarise(all_updates)

    logger.info("\n%s\nOPENAI RAW RESPONSE:\n%s\n%s", "-" * 60, raw.strip(), "-" * 60)

    try:
        parsed = json.loads(raw)
    except Exception as e:
        logger.warning("Failed to parse summary JSON, fallback mode: %s", e)
        parsed = {
            "summary": raw,
            "important_alerts": []
        }

    summary = parsed["summary"]
    alerts = parsed["important_alerts"]

    write_digest(summary, alerts)
    write_discoveries(extract_discoveries(summary))

    logger.info("Pipeline complete")


if __name__ == "__main__":
    run()