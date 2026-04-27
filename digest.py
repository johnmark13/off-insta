import os
import logging
import requests
from notion_client import Client
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

INTERESTS_DB_ID = os.getenv("INTERESTS_DB_ID")
DIGEST_DB_ID = os.getenv("DIGEST_DB_ID")
DISCOVERY_DB_ID = os.getenv("DISCOVERY_DB_ID")

notion = Client(auth=NOTION_TOKEN)
ai = OpenAI(api_key=OPENAI_API_KEY)


# ----------------------------
# 1. Get Interests from Notion
# ----------------------------
def get_interests():
    logger.info("Fetching interests from Notion")
    results = notion.databases.query(
        database_id=INTERESTS_DB_ID,
        filter={}
    )
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
# 2. Simple “fresh info” fetch
# (placeholder web search layer)
# ----------------------------
def fetch_updates(name):
    # Minimal v1: use a lightweight web search via API or placeholder
    # You can upgrade this later to Tavily / SerpAPI / News API
    logger.info("Fetching updates for: %s", name)
    query = f"{name} latest news tour interview 2026"

    response = requests.get(
        "https://api.duckduckgo.com/",
        params={"q": query, "format": "json"}
    )

    data = response.json()

    return data.get("AbstractText", "") or data.get("RelatedTopics", "")


# ----------------------------
# 3. AI summariser
# ----------------------------
def summarise(all_updates):
    logger.info("Generating summary via OpenAI")
    prompt = f"""
You are a cultural assistant.

From the following raw updates, create a concise daily digest.

Rules:
- Only include meaningful updates (releases, tours, collaborations, announcements)
- Ignore generic noise
- Highlight London relevance if present
- Keep it structured

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
    logger.info("Extracting discoveries via OpenAI")
    prompt = f"""
From this digest, extract NEW people/artists mentioned
that are NOT the main subjects.

Return JSON list with:
- name
- reason
- source
- confidence (low/medium/high)

TEXT:
{text}
"""

    response = ai.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content


# ----------------------------
# 5. Write Daily Digest to Notion
# ----------------------------
def write_digest(summary, alerts=""):
    logger.info("Writing digest to Notion")
    today = datetime.now().strftime("%Y-%m-%d")

    notion.pages.create(
        parent={"database_id": DIGEST_DB_ID},
        properties={
            "Date": {"date": {"start": today}},
            "Important Alerts": {"rich_text": [{"text": {"content": alerts}}]},
            "Summary": {"rich_text": [{"text": {"content": summary}}]}
        }
    )


# ----------------------------
# 6. Write discovery items
# ----------------------------
def write_discoveries(discoveries_json):
    # very simple parser (you can improve later)
    import json
    logger.info("Writing discoveries to Notion")
    try:
        items = json.loads(discoveries_json)
    except json.JSONDecodeError:
        logger.warning("Failed to parse discoveries JSON")
        return

    logger.info("Writing %d discovery items", len(items))
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
    logger.info("Starting digest pipeline")
    interests = get_interests()

    all_updates = ""

    for i in interests:
        update = fetch_updates(i["name"])
        all_updates += f"\n\n{i['name']}:\n{update}"

    summary = summarise(all_updates)
    discoveries = extract_discoveries(summary)

    write_digest(summary)
    write_discoveries(discoveries)

    logger.info("Digest pipeline complete")


if __name__ == "__main__":
    run()