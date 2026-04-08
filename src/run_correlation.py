#!/usr/bin/env python3
"""
run_correlation.py — Pull fresh leads from Postgres, fire correlated multi-state alerts.

Run after all state bots have completed their runs (e.g. as a cron at 3 PM):
  0 15 * * * cd ~/digital-scout-saas && venv/bin/python src/run_correlation.py >> logs/correlation.log 2>&1

Or invoke directly from a bot's main_{state}.py after all other bots have run.
"""

import logging
import os
import sys
from datetime import datetime, date, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

# Setup path so this can run standalone
sys.path.insert(0, os.path.dirname(__file__))

from correlator import Correlator, store_correlation
from matcher import match
from prompt_registry import get_active_prompt
from slack_dispatcher import SlackDispatcher


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.expanduser("~/digital-scout-saas/logs/correlation.log")
        ),
    ],
)
log = logging.getLogger(__name__)

DB_URL = os.environ.get("DATABASE_URL", "postgresql:///digital_scout")

# Webhook per state — keyed by channel name, loaded from .env
WEBHOOK_BY_STATE = {
    "NM": os.environ.get("SLACK_WEBHOOK_NM"),
    "TX": os.environ.get("SLACK_WEBHOOK_TX"),
    "LA": os.environ.get("SLACK_WEBHOOK_LA"),
    "OK": os.environ.get("SLACK_WEBHOOK_OK"),
    "WY": os.environ.get("SLACK_WEBHOOK_WY"),
    "ND": os.environ.get("SLACK_WEBHOOK_ND"),
}


def get_connection():
    return psycopg2.connect(DB_URL)


def fetch_recent_leads(days: int = 7) -> list[dict]:
    """Fetch all leads from the last N days across all states."""
    query = """
        SELECT id, api_number, state, operator_name, well_name, county,
               well_type, status, total_depth_ft, formation, spud_date,
               is_high_pressure, has_h2s_risk, is_directional, processed_at
        FROM leads
        WHERE processed_at >= NOW() - INTERVAL '%s days'
        ORDER BY processed_at DESC
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (days,))
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_prospects() -> list[dict]:
    """Fetch active prospects from Postgres."""
    query = """
        SELECT id, name, products, counties, formations, website, tier
        FROM prospects WHERE is_active = TRUE
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_active_prompt(state: str = "ALL") -> str:
    """Get the active correlated brief prompt."""
    p = get_active_prompt(state, "correlated")
    if p:
        return p.user_template
    # Fallback minimal template
    return "Summarize these correlated multi-state drilling leads.\n\n{correlated_leads}"


def build_correlated_brief(group: list[dict], template: str) -> str:
    """Fill the correlated brief template with lead data."""
    lead_lines = []
    for lead in group:
        states = lead.get("state", "?")
        county = lead.get("county", "?")
        operator = lead.get("operator_name", "?")
        formation = lead.get("formation") or "N/A"
        depth = lead.get("total_depth_ft") or 0
        hp = "HP" if lead.get("is_high_pressure") else ""
        h2s = "H2S" if lead.get("has_h2s_risk") else ""
        depth_str = f"{depth:,}" if depth else "?"

        flags = " ".join([x for x in [hp, h2s] if x])
        lead_lines.append(
            f"- [{states}] {operator} — {county} County | "
            f"{formation} | {depth_str} ft {flags}"
        )

    body = "\n".join(lead_lines)
    return template.format(correlated_leads=body)


def best_webhook_for_group(group: list[dict]) -> str | None:
    """
    Pick the webhook for the state with the most leads in this group.
    Falls back to NM webhook.
    """
    from collections import Counter
    state_counts = Counter(lead.get("state") for lead in group)
    primary_state = state_counts.most_common(1)[0][0]
    return WEBHOOK_BY_STATE.get(primary_state) or WEBHOOK_BY_STATE.get("NM")


def store_delivery(
    user_id: int | None,
    lead_id: int | None,
    correlation_id: int | None,
    channel: str,
    status: str,
    error_message: str | None = None,
) -> None:
    query = """
        INSERT INTO deliveries (user_id, lead_id, correlation_id, channel, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (user_id, lead_id, correlation_id, channel, status, error_message))
                conn.commit()
    except Exception as e:
        log.warning(f"Failed to record delivery: {e}")


def run():
    log.info("=== Correlation run started ===")
    start = datetime.now()

    # Fetch leads and prospects
    leads = fetch_recent_leads(days=7)
    prospects = fetch_prospects()
    log.info(f"Fetched {len(leads)} leads, {len(prospects)} prospects")

    if not leads:
        log.info("No leads to correlate.")
        return

    # Correlate
    c = Correlator()
    for lead in leads:
        c.add(lead)

    groups = c.get_groups(min_states=2)  # only multi-state
    log.info(f"Found {len(groups)} correlated multi-state groups")

    if not groups:
        log.info("No correlated groups found.")
        return

    # Template
    template = fetch_active_prompt("ALL")

    dispatcher = SlackDispatcher()

    for group in groups:
        log.info(
            f"Processing: {group.operator_name} — "
            f"{group.states} ({group.lead_count} leads)"
        )

        # Score all prospects against all leads in the group
        # Take the best matches across the group
        all_matches = []
        for lead in group.leads:
            scored = match(lead, prospects, top_n=5)
            for m in scored:
                if m not in all_matches:
                    all_matches.append(m)

        # Take top 5 unique prospects
        unique_matches = []
        seen_prospect_ids = set()
        for m in sorted(all_matches, key=lambda x: -x.score):
            if m.prospect_id not in seen_prospect_ids:
                seen_prospect_ids.add(m.prospect_id)
                unique_matches.append({
                    "prospect_id": m.prospect_id,
                    "name": m.name,
                    "products": m.products,
                    "tier": m.tier,
                    "score": m.score,
                    "score_breakdown": m.score_breakdown,
                })
        top5 = unique_matches[:5]

        # Build correlated brief
        brief = build_correlated_brief(group.leads, template)

        # Star if tier-1 prospects matched
        starred = any(m["tier"] == 1 for m in top5)

        # Pick webhook
        webhook_url = best_webhook_for_group(group.leads)

        if not webhook_url:
            log.warning("No webhook URL for group, skipping.")
            continue

        # Store correlation in Postgres
        try:
            with get_connection() as conn:
                corr_id = store_correlation(group, conn)
                conn.commit()
        except Exception as e:
            log.error(f"Failed to store correlation: {e}")
            corr_id = None

        # Dispatch correlated alert
        result = dispatcher.dispatch_correlated(
            group=group,
            brief=brief,
            matched_prospects=top5,
            starred=starred,
            webhook_url=webhook_url,
        )

        if result.success:
            log.info(f"Dispatched correlated alert: {group.correlation_key}")
            store_delivery(
                user_id=None,
                lead_id=None,
                correlation_id=corr_id,
                channel="slack",
                status="delivered",
            )
        else:
            log.error(f"Dispatch failed: {result.error}")
            store_delivery(
                user_id=None,
                lead_id=None,
                correlation_id=corr_id,
                channel="slack",
                status="failed",
                error_message=result.error,
            )

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"=== Correlation run complete in {elapsed:.1f}s ===")


if __name__ == "__main__":
    run()
