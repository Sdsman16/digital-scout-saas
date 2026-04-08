"""
correlator.py — Group leads across states by normalized operator name.

This is what makes Digital Scout different from any competitor:
same operator filing in multiple states within the same week = one
correlated alert instead of three separate ones.
"""

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date


OPERATOR_SUFFIXES = [
    " LLC", " LP", " L.P.", " LTD", " LTD.", " INC", " INC.",
    " CORPORATION", " CORP", " CORP.", " CO.", " CO",
    " HOLDINGS", " HOLDINGS INC", " OPERATING", " OPERATING LLC",
    " PARTNERS", " PARTNERS LP", " COMPANY", " ENERGY",
    " SERVICES", " GROUP", " VENTURES", " RESOURCES",
    " PETROLEUM", " OIL", " GAS", " EXPLORATION",
    " MIDSTREAM", " DOWNSTREAM", " PRODUCTION",
]

OPERATOR_PREFIXES = ["THE ", "THE"]


def normalize_operator(name: str) -> str:
    """
    Collapse operator names to a canonical key for correlation.

    Examples:
      "EXXON MOBIL CORP"        → "EXXON MOBIL"
      "ExxonMobil Oil Company"  → "EXXON MOBIL"
      "MARATHON OIL CORP"       → "MARATHON OIL"
      "MARATHON PETROLEUM"      → "MARATHON PETROLEUM"  (different entity)
      "CHEVRON U.S.A. INC"     → "CHEVRON USA"
      "COTAL ENERGY LLC"        → "COTAL ENERGY"
    """
    if not name:
        return ""

    n = name.upper().strip()

    # Remove prefixes
    for prefix in OPERATOR_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):].strip()

    # Remove suffixes
    for suffix in OPERATOR_SUFFIXES:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()

    # Collapse punctuation and whitespace
    n = re.sub(r"[-–—/,.]+", " ", n)
    n = re.sub(r"\s+", " ", n).strip()

    return n


def operator_fingerprint(name: str) -> str:
    """
    A stricter hash for grouping — removes numbers and very short words.
    Use when exact normalization produces false positives.
    """
    n = normalize_operator(name)
    # Remove numbers
    n = re.sub(r"\d+", "", n)
    # Remove single characters
    n = re.sub(r"\b\w\b", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def week_key(d: date | datetime | None) -> tuple[int, int]:
    """Return (ISO year, ISO week number) for a date."""
    if d is None:
        d = date.today()
    if isinstance(d, datetime):
        d = d.date()
    iso = d.isocalendar()
    return (iso[0], iso[1])


@dataclass
class CorrelatedGroup:
    correlation_key: str
    operator_name: str
    week: tuple[int, int]
    states: list[str]
    leads: list[dict]
    lead_count: int


class Correlator:
    """
    Correlate leads across states by operator + week.

    Usage:
        c = Correlator()
        for lead in leads:
            c.add(lead)
        groups = c.get_groups(min_states=2)  # only multi-state
    """

    def __init__(self):
        # key: (operator_normalized, year, week) → list of lead dicts
        self._groups: dict[tuple, list[dict]] = defaultdict(list)

    def add(self, lead: dict) -> None:
        """
        Add a lead to the correlation tracker.
        lead must have: operator_name, state, processed_at (or spud_date)
        """
        op = normalize_operator(lead.get("operator_name") or "")
        if not op:
            return

        processed = lead.get("processed_at")
        if isinstance(processed, str):
            try:
                processed = datetime.fromisoformat(processed.replace("Z", "+00:00")).date()
            except Exception:
                processed = date.today()
        elif isinstance(processed, datetime):
            processed = processed.date()
        elif processed is None:
            spud = lead.get("spud_date")
            if spud:
                try:
                    processed = datetime.strptime(str(spud)[:10], "%Y-%m-%d").date()
                except Exception:
                    processed = date.today()
            else:
                processed = date.today()

        year, week = week_key(processed)
        key = (op, year, week)
        self._groups[key].append(lead)

    def get_groups(self, min_states: int = 1) -> list[CorrelatedGroup]:
        """
        Return correlated groups with at least min_states different states.
        sorted by lead_count desc.
        """
        results = []
        for (op, year, week), leads in self._groups.items():
            states = list({lead.get("state") for lead in leads if lead.get("state")})
            if len(states) < min_states:
                continue

            # Use the most common/full operator name as display name
            operator_display = max(
                leads, key=lambda l: len(l.get("operator_name") or "")
            ).get("operator_name", op)

            results.append(CorrelatedGroup(
                correlation_key=f"{op}:{year}:W{week:02d}",
                operator_name=operator_display,
                week=(year, week),
                states=sorted(states),
                leads=leads,
                lead_count=len(leads),
            ))

        results.sort(key=lambda g: -len(g.states))
        return results


def store_correlation(group: CorrelatedGroup, conn) -> int:
    """
    Write a correlation group to Postgres.
    Updates existing or inserts new.
    Returns the correlation_id.
    """
    import psycopg2
    from psycopg2.extras import Json

    op = normalize_operator(group.operator_name)
    query = """
        INSERT INTO correlations
          (correlation_key, operator_name, week_number, year, states, lead_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (correlation_key) DO UPDATE SET
            lead_count = EXCLUDED.lead_count,
            states = EXCLUDED.states,
            created_at = NOW()
        RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(query, (
            group.correlation_key,
            group.operator_name,
            group.week[1],
            group.week[0],
            group.states,
            group.lead_count,
        ))
        corr_id = cur.fetchone()[0]

        # Link leads to this correlation
        for lead in group.leads:
            cur.execute(
                """UPDATE leads SET correlation_id = %s
                   WHERE api_number = %s AND state = %s""",
                (corr_id, lead.get("api_number"), lead.get("state")),
            )
    return corr_id
