# Digital Scout SaaS

Oil & gas permit intelligence — LLM-powered technical briefs, supplier matching, and Slack alerts for oilfield service companies.

## What this is

A SaaS layer on top of the [Digital Scout permit pipelines](https://github.com/Sdsman16) for TX, LA, NM, OK, WY, and ND. Takes raw permit data → enriches it → generates AI briefs → matches suppliers → delivers to Slack.

## Architecture

```
State ArcGIS APIs  ──►  Pipeline Workers  ──►  Postgres  ──►  Slack / Web App
                         (existing bots)         (this repo)     (Phase 2)
```

## What's built

- `migrations/001_initial.sql` — full Postgres schema (users, subscriptions, leads, matches, briefs, deliveries)
- `src/prompt_registry.py` — versioned prompt template store + CRUD + seed defaults
- `src/lead_store.py` — write/read processed leads to Postgres (bridges state bots)
- `src/matcher.py` — stateless lead-to-prospect matching across all states
- `src/correlator.py` — group leads by normalized operator name + week into correlated alerts
- `src/slack_dispatcher.py` — Block Kit message builder + exponential backoff retry + dedup
- `src/run_correlation.py` — correlation cron runner (fires at 3 PM after all state bots)
- `src/admin/app.py` — Flask admin UI for prompts and prospects at `localhost:5001`

## Setup

```bash
# 1. Clone
git clone https://github.com/Sdsman16/digital-scout-saas
cd digital-scout-saas

# 2. Python env
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Database
# Install Postgres (or use Supabase/RDS in production)
createdb digital_scout
psql $DATABASE_URL -f migrations/001_initial.sql

# 4. Configure
cp .env.example .env
# Edit .env with your DATABASE_URL

# 5. Seed prompts
python -c "from src.prompt_registry import seed_defaults; seed_defaults()"

# 6. Run admin UI
python -m src.admin.app
```

## Schema

| Table | Purpose |
|---|---|
| `prompt_templates` | Versioned LLM prompt store |
| `leads` | Every processed permit |
| `correlations` | Multi-state grouped leads |
| `prospects` | Supplier database |
| `matches` | Lead-to-prospect scores |
| `briefs` | Generated brief text |
| `users` | Web login |
| `subscriptions` | State subscriptions |
| `deliveries` | Slack/email delivery log |

## Build Phases

- [x] Phase 1: Prompt registry, lead store, matcher, Flask admin UI
- [x] Phase 2: Slack dispatcher with retries + multi-state correlation runner
- [x] Phase 3: Web login + lead dashboard ← DONE
- [ ] Phase 4: Stripe subscriptions
- [ ] Phase 5: AWS migration (ECS Fargate, RDS)

## License

Proprietary — all rights reserved.
