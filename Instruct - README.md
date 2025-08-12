# MissionSet Lite — FastAPI + SQLite + OpenSearch

Minimal platform to add data via a web form, save to a database, and search using OpenSearch.

## Run (Docker Compose)

```bash
docker compose up -d --build
```

- App: http://localhost:8000
- OpenSearch API: http://localhost:9200
- Data persists in volumes `osdata` and `appdata`.

## What this does

- **/data/new** — add an item (title, description, tags)
- Saves to **SQLite** (mounted at `/data/data.db` in the app container)
- Indexes each item into **OpenSearch** (index `missionset-data`)
- **/search** — queries OpenSearch across title, description, tags

## Notes

- OpenSearch runs **without security** for local development (`plugins.security.disabled=true`). Do **not** use this setting in production.
- To clean everything: `docker compose down -v`
- To watch logs: `docker compose logs -f`

## Env (optional)

You can change the index name or DB path by editing `docker-compose.yml` env vars:

- `OS_INDEX` (default `missionset-data`)
- `DATABASE_URL` (default `sqlite:////data/data.db`)