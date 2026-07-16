Status: Approved for implementation on 2026-04-27

Implementation kickoff (first build sprint):

1. Update digest.py summarise() JSON contract to include summary_bullets and sources_used.
2. Add parse/validate helpers with safe fallback defaults.
3. Add web-link ingestion function and merge with RSS items.
4. Add full report page creation + link in Daily Digest row.
5. Add discovery dedupe/update logic preserving Added to Watchlist.
6. Run end-to-end test against Notion schema and adjust property mappings.

## Plan: Off-Insta MVP Upgrade

Build an MVP that turns the current digest into a structured daily briefing system by adding a linked full report page, robust discovery dedup/watchlist behavior, richer summary output with bullets, and direct Web Link ingestion. Keep Instagram in phase 2 to avoid fragile/ToS-risk scraping in v1.

**Steps**

1. Phase 1: Data Contracts and Guardrails
2. Update summarization contract in digest.py summarise() to require strict JSON with fields: summary, summary_bullets (array), important_alerts (high-confidence events only), and sources_used.
3. Add parser/validator helpers in digest.py to normalize and safely fallback when AI JSON is malformed (preserve pipeline continuity).
4. Add runtime counters/logging for per-interest source counts and total alerts produced.
5. Phase 2: Source Ingestion (MVP)
6. Extend fetch pipeline in digest.py to ingest both Google News RSS and Interest Web Link content for each artist.
7. Add a dedicated web fetch function that tries feed parsing first, then basic HTML metadata/headline extraction as fallback.
8. Normalize all fetched items into one shared item shape (title, link, date, source_type, interest_name) before summarization so downstream logic is deterministic.
9. Maintain Google News as baseline fallback if Web Link fetch fails.
10. Phase 3: Daily Full Report Linked from Digest
11. Update write_digest() to create a digest row first, then create/attach a Notion page for full report content (linked via Full Report relation/url field based on actual schema).
12. Render full report blocks in human-readable sections: per-interest headings, source links, short extracted notes, and summary bullets.
13. Add image placeholders in report generation pipeline and leave image extraction behind a feature flag for stretch scope.
14. Phase 4: Discovery Intelligence (No Duplicates + Watchlist Semantics)
15. Refactor write_discoveries() to perform dedup by normalized Artist Name (trim/lower/collapse spaces), using database query plus in-run cache.
16. On duplicate discovery: update existing row with appended reason/source context and confidence merge (never downgrade), while preserving Added to Watchlist if true.
17. On new discovery: create row with Added to Watchlist false, normalized confidence value, and traceable source reference.
18. Add metrics logs: created vs updated discoveries and number of watchlisted rediscoveries.
19. Phase 5: Product Polish for Daily News Replacement
20. Add digest output sections for Why this matters today, Quick actions, and Skip list (noise filtered out), generated from same structured prompt.
21. Add optional notification-friendly plain-text brief output (for future email/phone workflows) without changing Notion schema.
22. Add staleness filtering (ignore repeated unchanged items across recent days) to reduce doom-scroll style noise.
23. Verification and Rollout
24. Local dry-run with mocked Notion/OpenAI responses to validate parser, dedupe logic, and report block structure.
25. End-to-end run against test Notion databases to verify field type compatibility (Date title, Important Alerts number, Full Report link field behavior).
26. Validate duplicate handling by replaying same discoveries across two runs and asserting update-not-create behavior.
27. Validate summary quality manually for 3 interests: bullet clarity, alert precision, and source grounding.
28. Deploy to scheduled workflow and review first 3 daily runs for reliability and noise level.

**Relevant files**

- c:/dev/code/johnmark13/off-insta/digest.py — primary implementation target: summarise(), fetch_updates(), write_digest(), write_discoveries(), run().
- c:/dev/code/johnmark13/off-insta/requirements.txt — add web ingestion dependencies for feeds/html parsing if selected.
- c:/dev/code/johnmark13/off-insta/README.md — update env vars, schema expectations, and run behavior.
- c:/dev/code/johnmark13/off-insta/.github/workflows/digest.yml — optional timeout/retry/logging adjustments after added fetch paths.
- c:/dev/code/johnmark13/off-insta/.github/agents/off-insta.agent.md — capture agreed TODO items and operating guidance.

**Verification**

1. Unit-level checks (or script-level assertions) for JSON parse/validation and fallback behavior.
2. Integration check that digest row writes with correct property types and full report link is populated.
3. Integration check that rediscovered artist updates the same Discovery page and preserves Added to Watchlist.
4. Manual quality pass on generated full report readability (headings, bullets, links).
5. Observability check in logs: fetched item totals, alert count, discovery created/updated counts.

**Decisions**

- Included in v1: MVP scope with linked Notion full report, web link ingestion, richer summary bullets, discovery dedupe/watchlist behavior.
- Excluded from v1: Instagram ingestion (deferred to phase 2 due reliability and policy constraints).
- Important alert definition: high-confidence events only (releases, tours, collaborations, official announcements).
- Watchlist behavior: preserve Added to Watchlist true on rediscovery; append new context only.

**Further Considerations**

1. Full Report field type confirmation: If field is relation, link created child page; if URL, store permalink. Recommendation: relation for stronger Notion-native navigation.
2. Duplicate key hardening: Add hidden normalized name property in Discoveries DB for exact matching and better query performance. Recommendation: include once schema migration is acceptable.
3. Noise suppression: Add 3-day novelty window keyed by source URL hash to avoid repeated headlines. Recommendation: include in MVP if implementation effort stays under 1 hour.
