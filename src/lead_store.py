"""
lead_store.py — Write and read processed leads from Postgres.

Bridges the existing local bots (which write processed_apis.json)
with the new SaaS Postgres layer. The SaaS layer reads what bots write.
"""

import os
import json
from datetime import date
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor, Json


def get_db_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://localhost:5432/digital_scout")


def get_connection():
    return psycopg2.connect(get_db_url())


# ── Write ──────────────────────────────────────────────────────────────────────

def store_lead(lead_data: dict) -> int:
    """
    Upsert a single lead into Postgres.
    Returns the lead ID.
    Skips if api_number + state already exists (idempotent).
    """
    query = """
        INSERT INTO leads (
            api_number, state, operator_name, well_name, county,
            well_type, status, latitude, longitude, total_depth_ft,
            formation, spud_date, is_high_pressure, has_h2s_risk,
            is_directional, is_pre_spud, raw_data, processed_at
        ) VALUES (
            %(api_number)s, %(state)s, %(operator_name)s, %(well_name)s,
            %(county)s, %(well_type)s, %(status)s,
            %(latitude)s, %(longitude)s, %(total_depth_ft)s,
            %(formation)s, %(spud_date)s, %(is_high_pressure)s,
            %(has_h2s_risk)s, %(is_directional)s, %(is_pre_spud)s,
            %(raw_data)s, NOW()
        )
        ON CONFLICT (api_number, state) DO UPDATE SET
            operator_name   = EXCLUDED.operator_name,
            well_name       = EXCLUDED.well_name,
            county          = EXCLUDED.county,
            well_type       = EXCLUDED.well_type,
            status          = EXCLUDED.status,
            latitude        = EXCLUDED.latitude,
            longitude       = EXCLUDED.longitude,
            total_depth_ft  = EXCLUDED.total_depth_ft,
            formation       = EXCLUDED.formation,
            spud_date       = EXCLUDED.spud_date,
            is_high_pressure   = EXCLUDED.is_high_pressure,
            has_h2s_risk      = EXCLUDED.has_h2s_risk,
            is_directional     = EXCLUDED.is_directional,
            is_pre_spud       = EXCLUDED.is_pre_spud,
            raw_data          = EXCLUDED.raw_data,
            processed_at       = NOW()
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, {
                "api_number":      lead_data.get("api_number", ""),
                "state":           lead_data.get("state", ""),
                "operator_name":   lead_data.get("operator_name"),
                "well_name":       lead_data.get("well_name"),
                "county":          lead_data.get("county"),
                "well_type":       lead_data.get("well_type"),
                "status":          lead_data.get("status"),
                "latitude":        lead_data.get("latitude"),
                "longitude":       lead_data.get("longitude"),
                "total_depth_ft":  lead_data.get("total_depth_ft"),
                "formation":       lead_data.get("formation"),
                "spud_date":      lead_data.get("spud_date"),
                "is_high_pressure":  lead_data.get("is_high_pressure", False),
                "has_h2s_risk":      lead_data.get("has_h2s_risk", False),
                "is_directional":     lead_data.get("is_directional", False),
                "is_pre_spud":       lead_data.get("is_pre_spud", False),
                "raw_data":        Json(lead_data.get("raw_data", {})),
            })
            lead_id = cur.fetchone()[0]
            conn.commit()
    return lead_id


def store_leads_batch(leads: list[dict]) -> int:
    """
    Batch upsert a list of leads.
    Returns the number of leads written.
    """
    if not leads:
        return 0
    query = """
        INSERT INTO leads (
            api_number, state, operator_name, well_name, county,
            well_type, status, latitude, longitude, total_depth_ft,
            formation, spud_date, is_high_pressure, has_h2s_risk,
            is_directional, is_pre_spud, raw_data, processed_at
        ) VALUES (
            %(api_number)s, %(state)s, %(operator_name)s, %(well_name)s,
            %(county)s, %(well_type)s, %(status)s,
            %(latitude)s, %(longitude)s, %(total_depth_ft)s,
            %(formation)s, %(spud_date)s, %(is_high_pressure)s,
            %(has_h2s_risk)s, %(is_directional)s, %(is_pre_spud)s,
            %(raw_data)s, NOW()
        )
        ON CONFLICT (api_number, state) DO UPDATE SET
            operator_name   = EXCLUDED.operator_name,
            well_name       = EXCLUDED.well_name,
            county          = EXCLUDED.county,
            well_type       = EXCLUDED.well_type,
            status          = EXCLUDED.status,
            latitude        = EXCLUDED.latitude,
            longitude       = EXCLUDED.longitude,
            total_depth_ft  = EXCLUDED.total_depth_ft,
            formation       = EXCLUDED.formation,
            spud_date       = EXCLUDED.spud_date,
            is_high_pressure   = EXCLUDED.is_high_pressure,
            has_h2s_risk      = EXCLUDED.has_h2s_risk,
            is_directional     = EXCLUDED.is_directional,
            is_pre_spud       = EXCLUDED.is_pre_spud,
            raw_data          = EXCLUDED.raw_data,
            processed_at       = NOW()
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for lead in leads:
                cur.execute(query, {
                    "api_number":      lead.get("api_number", ""),
                    "state":           lead.get("state", ""),
                    "operator_name":   lead.get("operator_name"),
                    "well_name":       lead.get("well_name"),
                    "county":          lead.get("county"),
                    "well_type":       lead.get("well_type"),
                    "status":          lead.get("status"),
                    "latitude":        lead.get("latitude"),
                    "longitude":       lead.get("longitude"),
                    "total_depth_ft":  lead.get("total_depth_ft"),
                    "formation":       lead.get("formation"),
                    "spud_date":      lead.get("spud_date"),
                    "is_high_pressure":  lead.get("is_high_pressure", False),
                    "has_h2s_risk":      lead.get("has_h2s_risk", False),
                    "is_directional":     lead.get("is_directional", False),
                    "is_pre_spud":       lead.get("is_pre_spud", False),
                    "raw_data":        Json(lead.get("raw_data", {})),
                })
            conn.commit()
    return len(leads)


def store_brief(lead_id: int, brief_text: str, model_used: str, token_count: int | None = None) -> int:
    """Store a generated brief for a lead."""
    query = """
        INSERT INTO briefs (lead_id, brief_text, model_used, token_count, generated_at)
        VALUES (%s, %s, %s, %s, NOW())
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lead_id, brief_text, model_used, token_count))
            brief_id = cur.fetchone()[0]
            conn.commit()
    return brief_id


def store_matches(lead_id: int, matches: list[dict]) -> int:
    """
    Store lead-to-prospect matches.
    matches: list of {prospect_id, score, score_breakdown, rank}
    """
    query = """
        INSERT INTO matches (lead_id, prospect_id, score, score_breakdown, rank)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (lead_id, prospect_id) DO UPDATE SET
            score = EXCLUDED.score,
            score_breakdown = EXCLUDED.score_breakdown,
            rank = EXCLUDED.rank
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for m in matches:
                cur.execute(query, (
                    lead_id,
                    m["prospect_id"],
                    m["score"],
                    Json(m.get("score_breakdown", {})),
                    m["rank"],
                ))
            conn.commit()
    return len(matches)


# ── User / Subscription helpers ───────────────────────────────────────────────

def get_user_by_email(email: str) -> dict | None:
    """Fetch a user dict by email."""
    query = """
        SELECT id, email, hashed_password, company, is_admin
        FROM users WHERE email = %s
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (email.lower().strip(),))
            row = cur.fetchone()
    return dict(row) if row else None


def get_subscriptions_for_user(user_id: int) -> list[str]:
    """Return list of active state subscription codes for a user."""
    query = """
        SELECT state FROM subscriptions
        WHERE user_id = %s AND status = 'active'
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id,))
            return [row[0] for row in cur.fetchall()]


def get_dashboard_leads(user_id: int, days: int = 7, limit: int = 100, query: str = "") -> list[dict]:
    """
    Fetch leads for a user's subscribed states.
    Falls back to all states if user has no subscriptions.
    Supports free-text search across operator, county, formation, api_number.
    """
    states = get_subscriptions_for_user(user_id)

    if not states:
        return get_recent_leads(state=None, days=days, limit=limit, query=query)

    search_clause = ""
    args = [states, days, limit]
    if query:
        search_clause = "AND ("
        for col in ["l.operator_name", "l.county", "l.formation", "l.api_number"]:
            search_clause += f"OR {col} ILIKE %s "
        search_clause += ")"
        args += [f"%{query}%"] * 4

    sql = f"""
        SELECT l.*, b.brief_text,
               array_agg(jsonb_build_object(
                   'name', p.name, 'tier', p.tier, 'rank', m.rank,
                   'score', m.score))
                   FILTER (WHERE m.id IS NOT NULL) AS matched_prospects
        FROM leads l
        LEFT JOIN matches m ON m.lead_id = l.id
        LEFT JOIN prospects p ON p.id = m.prospect_id
        LEFT JOIN briefs b ON b.lead_id = l.id
        WHERE l.state = ANY(%s)
          AND l.processed_at >= NOW() - INTERVAL '%s days'
          {search_clause}
        GROUP BY l.id, b.brief_text
        ORDER BY l.processed_at DESC
        LIMIT %s
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


# ── Read ───────────────────────────────────────────────────────────────────────

def get_lead(api_number: str, state: str) -> dict | None:
    """Fetch a single lead by API + state."""
    query = """
        SELECT l.*, b.brief_text
        FROM leads l
        LEFT JOIN briefs b ON b.lead_id = l.id
        WHERE l.api_number = %s AND l.state = %s
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (api_number, state))
            row = cur.fetchone()
    return dict(row) if row else None


def get_recent_leads(state: str | None = None, days: int = 7, limit: int = 100, query: str = "") -> list[dict]:
    """Fetch recent leads, optionally filtered by state. Supports free-text search."""
    search_clause = ""
    args_list = []
    if query:
        for col in ["l.operator_name", "l.county", "l.formation", "l.api_number"]:
            search_clause += f"AND ({col} ILIKE %s) "
        args_list += [f"%{query}%"] * 4

    if state:
        base = f"""
            SELECT l.*, b.brief_text,
                   array_agg(jsonb_build_object(
                       'name', p.name, 'tier', p.tier, 'rank', m.rank,
                       'score', m.score))
                       FILTER (WHERE m.id IS NOT NULL) AS matched_prospects
            FROM leads l
            LEFT JOIN matches m ON m.lead_id = l.id
            LEFT JOIN prospects p ON p.id = m.prospect_id
            LEFT JOIN briefs b ON b.lead_id = l.id
            WHERE l.state = %s
              AND l.processed_at >= NOW() - INTERVAL '%s days'
              {search_clause}
            GROUP BY l.id, b.brief_text
            ORDER BY l.processed_at DESC
            LIMIT %s
        """
        args = (state, days, *args_list, limit)
    else:
        base = f"""
            SELECT l.*, b.brief_text,
                   array_agg(jsonb_build_object(
                       'name', p.name, 'tier', p.tier, 'rank', m.rank,
                       'score', m.score))
                       FILTER (WHERE m.id IS NOT NULL) AS matched_prospects
            FROM leads l
            LEFT JOIN matches m ON m.lead_id = l.id
            LEFT JOIN prospects p ON p.id = m.prospect_id
            LEFT JOIN briefs b ON b.lead_id = l.id
            WHERE l.processed_at >= NOW() - INTERVAL '%s days'
              {search_clause}
            GROUP BY l.id, b.brief_text
            ORDER BY l.processed_at DESC
            LIMIT %s
        """
        args = (days, *args_list, limit)

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(base, args)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_correlated_leads(correlation_id: int) -> list[dict]:
    """Fetch all leads in a correlated group."""
    query = """
        SELECT l.* FROM leads l
        WHERE l.correlation_id = %s
        ORDER BY l.state
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (correlation_id,))
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_lead_detail(api_number: str, state: str) -> dict | None:
    """Full lead detail: lead row + brief + all matched prospects with full scores."""
    query = """
        SELECT l.*, b.brief_text, b.model_used, b.token_count, b.generated_at
        FROM leads l
        LEFT JOIN briefs b ON b.lead_id = l.id
        WHERE l.api_number = %s AND l.state = %s
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (api_number, state))
            row = cur.fetchone()
    if not row:
        return None
    lead = dict(row)

    # Fetch all matched prospects with score breakdown
    match_query = """
        SELECT m.score, m.score_breakdown, m.rank,
               p.name, p.products, p.website, p.tier
        FROM matches m
        JOIN prospects p ON p.id = m.prospect_id
        WHERE m.lead_id = %s
        ORDER BY m.rank
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(match_query, (lead["id"],))
            lead["prospect_details"] = [dict(r) for r in cur.fetchall()]
    return lead


def get_recent_correlations(days: int = 7, limit: int = 50) -> list[dict]:
    """
    Fetch recent correlated groups from the last N days.
    Returns correlation records with operator, week, states, lead_count.
    """
    query = """
        SELECT c.*,
               array_agg(jsonb_build_object(
                   'api_number', l.api_number,
                   'state', l.state,
                   'county', l.county,
                   'formation', l.formation,
                   'total_depth_ft', l.total_depth_ft,
                   'is_high_pressure', l.is_high_pressure,
                   'has_h2s_risk', l.has_h2s_risk,
                   'is_directional', l.is_directional,
                   'operator_name', l.operator_name,
                   'brief_text', b.brief_text
               )) FILTER (WHERE l.id IS NOT NULL) AS leads
        FROM correlations c
        LEFT JOIN leads l ON l.correlation_id = c.id
        LEFT JOIN briefs b ON b.lead_id = l.id
        WHERE c.created_at >= NOW() - INTERVAL '%s days'
        GROUP BY c.id
        ORDER BY c.created_at DESC
        LIMIT %s
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (days, limit))
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_correlation_detail(correlation_id: int) -> dict | None:
    """Fetch a single correlation with full lead details."""
    query = """
        SELECT c.*,
               array_agg(jsonb_build_object(
                   'api_number', l.api_number,
                   'state', l.state,
                   'county', l.county,
                   'well_name', l.well_name,
                   'formation', l.formation,
                   'total_depth_ft', l.total_depth_ft,
                   'is_high_pressure', l.is_high_pressure,
                   'has_h2s_risk', l.has_h2s_risk,
                   'is_directional', l.is_directional,
                   'is_pre_spud', l.is_pre_spud,
                   'operator_name', l.operator_name,
                   'processed_at', l.processed_at,
                   'brief_text', b.brief_text
               )) FILTER (WHERE l.id IS NOT NULL) AS leads
        FROM correlations c
        LEFT JOIN leads l ON l.correlation_id = c.id
        LEFT JOIN briefs b ON b.lead_id = l.id
        WHERE c.id = %s
        GROUP BY c.id
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (correlation_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def upsert_subscription(user_id: int, state: str, tier: str = "basic", status: str = "active") -> dict:
    """
    Insert or update a subscription for a user+state.
    """
    query = """
        INSERT INTO subscriptions (user_id, state, tier, status, current_period_end)
        VALUES (%s, %s, %s, %s, NOW() + INTERVAL '1 month')
        ON CONFLICT (user_id, state) DO UPDATE SET
            tier = EXCLUDED.tier,
            status = EXCLUDED.status,
            updated_at = NOW()
        RETURNING id, user_id, state, tier, status, current_period_end
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id, state, tier, status))
            row = cur.fetchone()
            conn.commit()
    return {"id": row[0], "user_id": row[1], "state": row[2], "tier": row[3], "status": row[4], "current_period_end": row[5]}


def delete_subscription(user_id: int, state: str) -> None:
    """Remove a subscription."""
    query = "DELETE FROM subscriptions WHERE user_id = %s AND state = %s"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id, state))
            conn.commit()
