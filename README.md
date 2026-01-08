## Lexis Daily Case Collector

Runs a daily Playwright job to export filtered Lexis alerts.

### How it runs
- Deployed as a Docker-based Cron Job on Render
- Uses environment variables (no .env in repo)

### Required Environment Variables
See `.env.example`

### Output
JSON files are generated per run.
