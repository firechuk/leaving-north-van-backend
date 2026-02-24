# Leaving North Van Backend

Traffic ingestion + API service for North Vancouver.

Canonical project handoff doc:

- `/Users/chuk/Projects/LeavingNorthVan/PROJECT_STATE.md`

## Current Stack

- Node.js + Express
- HERE Traffic Flow API (primary traffic source)
- PostgreSQL via `DATABASE_URL` (Railway)
- In-memory fallback mode if DB is unavailable

## Runtime Behavior

- Snapshot cadence: every 2 minutes
- Service day boundary: midnight Vancouver time (`0:00`)
- Per-snapshot data includes tracked segments + counterflow state
- `/api/traffic/today` supports variable history window via `serviceDays`

## API Endpoints

- `GET /health`
- `GET /api/traffic/today?serviceDays=N&refresh=1`
- `GET /api/database/stats`
- `GET /api/debug/routes`
- `GET /api/counterflow/status` (use `?debug=1` for parser diagnostics)
- `GET /api/counterflow/history`

## Environment Variables

- `HERE_API_KEY` (required for live traffic ingestion)
- `DATABASE_URL` (recommended for persistence)
- `PORT` (Railway injects automatically)
- `NODE_ENV`

## Local Run

```bash
cd /Users/chuk/Projects/LeavingNorthVan/leaving-north-van-backend
npm install
npm start
```

## Deployment

Railway deploys from `main` on push:

```bash
cd /Users/chuk/Projects/LeavingNorthVan/leaving-north-van-backend
git add .
git commit -m "Describe change"
git push origin main
```

## Notes

- Counterflow parser relies on BC ATIS source pages and can be inspected from `/api/counterflow/status?debug=1`.
- Debug segment tooling expects `/api/debug/routes` and is consumed by the frontend debug HTML page.
