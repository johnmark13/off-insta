# 🎧 Culture Digest AI

A personal AI-powered system that replaces social media scrolling with a **daily, structured cultural briefing**.

It automatically tracks artists, writers, and creators you care about, summarises relevant updates, and discovers new people through collaborations, tours, and recommendations.

Everything is delivered into Notion as a structured knowledge base.

---

# ✨ What it does

Every morning at 8AM:

- Reads your **Notion Interests database**
- Fetches the latest updates (news, web, announcements)
- Uses AI to filter noise and extract meaningful events
- Writes a structured **Daily Digest** to Notion
- Extracts **new discovered creators** into a Discovery database

---

# 🧠 Key idea

Instead of:

> endlessly scrolling Instagram to stay updated

You get:

> a curated, once-a-day intelligence briefing about your cultural world

---

# 🏗️ System architecture

```text
Notion (Interests database)
        ↓
GitHub Actions (scheduled 8AM job)
        ↓
Python script (digest pipeline)
        ↓
Web + news retrieval
        ↓
OpenAI (filter + summarise + extract insights)
        ↓
Notion (Daily Digest + Discovery)
```
