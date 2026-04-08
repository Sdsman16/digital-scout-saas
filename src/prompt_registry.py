"""
prompt_registry.py — Read/write prompt templates from Postgres.

The prompt registry is the central store for all LLM prompt templates.
Each state can have its own template, or share one via state='ALL'.
Templates are versioned — updating creates a new version, keeping history.
"""

import os
from dataclasses import dataclass
from datetime import datetime

import yaml
import psycopg2
from psycopg2.extras import RealDictCursor


@dataclass
class PromptTemplate:
    id: int
    state: str
    template_type: str
    version: int
    system_prompt: str
    user_template: str
    description: str | None
    is_active: bool
    created_at: datetime


def get_db_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://localhost:5432/digital_scout")


def get_connection():
    return psycopg2.connect(get_db_url())


# ── Read ────────────────────────────────────────────────────────────────────────

def get_active_prompt(state: str, template_type: str) -> PromptTemplate | None:
    """
    Get the active prompt for a given state and type.
    Falls back to 'ALL' if no state-specific prompt exists.
    """
    query = """
        SELECT id, state, template_type, version, system_prompt,
               user_template, description, is_active, created_at
        FROM prompt_templates
        WHERE (state = %s OR state = 'ALL')
          AND template_type = %s
          AND is_active = TRUE
        ORDER BY
            CASE WHEN state = %s THEN 0 ELSE 1 END,
            version DESC
        LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (state, template_type, state))
            row = cur.fetchone()
    if not row:
        return None
    return PromptTemplate(**dict(row))


def get_all_prompts(state: str | None = None) -> list[PromptTemplate]:
    """Get all prompt versions, optionally filtered by state."""
    if state:
        query = """
            SELECT id, state, template_type, version, system_prompt,
                   user_template, description, is_active, created_at
            FROM prompt_templates
            WHERE state = %s OR state = 'ALL'
            ORDER BY state, template_type, version DESC
        """
        args = (state,)
    else:
        query = """
            SELECT id, state, template_type, version, system_prompt,
                   user_template, description, is_active, created_at
            FROM prompt_templates
            ORDER BY state, template_type, version DESC
        """
        args = ()

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, args)
            rows = cur.fetchall()
    return [PromptTemplate(**dict(r)) for r in rows]


# ── Write ──────────────────────────────────────────────────────────────────────

def upsert_prompt(
    state: str,
    template_type: str,
    system_prompt: str,
    user_template: str,
    description: str | None = None,
    version: int | None = None,
) -> PromptTemplate:
    """
    Insert or update a prompt template.

    If version is None: creates a new version (highest existing + 1).
    If version is given: upserts that specific version.
    Deactivates the previous active version for this state/type.
    """
    if version is None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COALESCE(MAX(version), 0) + 1 AS next_v
                       FROM prompt_templates
                       WHERE state = %s AND template_type = %s""",
                    (state, template_type),
                )
                version = cur.fetchone()[0]

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Deactivate previous active version
            cur.execute(
                """UPDATE prompt_templates
                   SET is_active = FALSE, updated_at = NOW()
                   WHERE state = %s AND template_type = %s AND is_active = TRUE""",
                (state, template_type),
            )

            # Upsert new version
            cur.execute(
                """INSERT INTO prompt_templates
                   (state, template_type, version, system_prompt, user_template,
                    description, is_active, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
                   ON CONFLICT (state, template_type, version)
                   DO UPDATE SET
                       system_prompt = EXCLUDED.system_prompt,
                       user_template = EXCLUDED.user_template,
                       description = EXCLUDED.description,
                       updated_at = NOW()
                   RETURNING id, state, template_type, version, system_prompt,
                             user_template, description, is_active, created_at""",
                (state, template_type, version, system_prompt,
                 user_template, description),
            )
            row = cur.fetchone()
            conn.commit()

    return PromptTemplate(
        id=row[0], state=row[1], template_type=row[2],
        version=row[3], system_prompt=row[4], user_template=row[5],
        description=row[6], is_active=True, created_at=row[8],
    )


# ── Seed defaults ──────────────────────────────────────────────────────────────

DEFAULT_PROMPTS = {
    "brief": {
        "system_prompt": (
            "You are an oil & gas intelligence analyst drafting a Technical Brief "
            "for a specialized supplier. IMPORTANT: Do NOT use markdown tables. "
            "Use bullet points (•) and bold text only. Keep under 400 words."
        ),
        "user_template": """Based on the following permit data, write a Technical Brief:

PERMIT DATA:
{permit_data}

Draft the brief using this structure — plain text only, no tables:

## {state} Drilling Lead: {county} County (Well #{well_name})

**API:** {api_number}
**Operator:** {operator_name}
**County:** {county}
**Formation:** {formation}

Summary: {summary}

Key Intel:
{key_intel}

Recommended Products:
{recommended_products}

Operator Contact:
{operator_contact}

---
*Generated by Digital Scout — {state} Permit Intelligence Pipeline*""",
    },
    "correlated": {
        "system_prompt": (
            "You are an oil & gas intelligence analyst summarizing correlated "
            "multi-state drilling activity for a specialized supplier."
        ),
        "user_template": """The following permits were filed by the same operator across multiple states this week:

{correlated_leads}

Write a concise executive summary highlighting:
1. The operator and their activity pattern
2. Equipment implications across all states
3. Priority supplier recommendations

Keep under 300 words. No tables — use bullet points and bold text.""",
    },
}


def seed_defaults():
    """Seed the database with default prompt templates if none exist."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM prompt_templates")
            if cur.fetchone()[0] > 0:
                return  # Already seeded

    for state in ["TX", "NM", "OK", "WY", "ND", "LA", "ALL"]:
        for template_type, content in DEFAULT_PROMPTS.items():
            upsert_prompt(
                state=state,
                template_type=template_type,
                system_prompt=content["system_prompt"],
                user_template=content["user_template"],
                description=f"Default {template_type} prompt template",
            )
    print("Seed complete.")
