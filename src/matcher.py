"""
matcher.py — Stateless lead-to-prospect matching.

Moved from the per-state modules. One unified function that works
across all states by accepting a normalized lead dict.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class MatchResult:
    prospect_id: int
    name: str
    products: list[str]
    tier: int
    score: float
    score_breakdown: dict[str, float]
    rank: int


def normalize_operator(name: str) -> str:
    """
    Normalize operator names for correlation matching.
    Strips common noise: spaces, suffixes like LLC/Ltd/Corp/Inc.
    """
    if not name:
        return ""
    n = name.upper().strip()
    # Remove common business suffixes
    for suffix in [" LLC", " LP", " LP.", " LTD", " LTD.", " INC",
                   " INC.", " CORPORATION", " CORP", " CORP.",
                   " HOLDINGS", " OPERATING", " PARTNERS", " COMPANY"]:
        n = n.replace(suffix, "")
    # Collapse whitespace
    import re
    n = re.sub(r"\s+", " ", n).strip()
    return n


def score_prospect(prospect: dict, lead: dict) -> tuple[float, dict[str, float]]:
    """
    Score a single prospect against a normalized lead dict.
    Returns (total_score, score_breakdown).
    """
    score = 0.0
    breakdown = {}

    # County match (+3)
    lead_county = (lead.get("county") or "").lower().strip()
    counties = prospect.get("counties", [])
    county_hit = False
    if "all" in [c.lower() for c in counties]:
        score += 1
        breakdown["county_all"] = 1.0
        county_hit = True
    elif any(c.lower() in lead_county or lead_county in c.lower()
             for c in counties):
        score += 3
        breakdown["county"] = 3.0
        county_hit = True
    if not county_hit:
        breakdown["county"] = 0.0

    # Formation match (+3)
    lead_form = (lead.get("formation") or "").lower().strip()
    formations = prospect.get("formations", [])
    form_hit = False
    if "all" in [f.lower() for f in formations]:
        score += 1
        breakdown["formation_all"] = 1.0
        form_hit = True
    elif any(f.lower() in lead_form or lead_form in f.lower()
             for f in formations):
        score += 3
        breakdown["formation"] = 3.0
        form_hit = True
    if not form_hit:
        breakdown["formation"] = 0.0

    # H2S bonus (+2)
    h2s_bonus = 0.0
    if lead.get("has_h2s_risk"):
        products = prospect.get("products", [])
        if any("h2s" in p.lower() or "sour" in p.lower() for p in products):
            h2s_bonus = 2.0
            score += 2.0
    breakdown["h2s"] = h2s_bonus

    # High pressure bonus (+2)
    hp_bonus = 2.0 if lead.get("is_high_pressure") else 0.0
    score += hp_bonus
    breakdown["high_pressure"] = hp_bonus

    # Depth bonus (+1)
    depth = lead.get("total_depth_ft") or 0
    depth_bonus = 1.0 if depth >= 10000 else 0.0
    score += depth_bonus
    breakdown["depth"] = depth_bonus

    # Directional bonus (+1)
    dir_bonus = 1.0 if lead.get("is_directional") else 0.0
    score += dir_bonus
    breakdown["directional"] = dir_bonus

    # Tier penalty
    tier = prospect.get("tier", 99)
    if tier <= 3:
        score -= tier * 0.5
        breakdown["tier_penalty"] = tier * 0.5
    else:
        breakdown["tier_penalty"] = 0.0

    return score, breakdown


def match(lead: dict, prospects: list[dict], top_n: int = 5) -> list[MatchResult]:
    """
    Score all prospects against a normalized lead dict.
    Returns top N matches sorted by score desc, then tier asc.
    """
    scored = []
    for p in prospects:
        total_score, breakdown = score_prospect(p, lead)
        scored.append((total_score, p.get("tier", 99), p, breakdown))

    scored.sort(key=lambda x: (-x[0], x[1]))

    results = []
    for rank, (total_score, _, p, breakdown) in enumerate(scored[:top_n], start=1):
        results.append(MatchResult(
            prospect_id=p["id"],
            name=p["name"],
            products=p.get("products", []),
            tier=p.get("tier", 99),
            score=round(total_score, 2),
            score_breakdown=breakdown,
            rank=rank,
        ))
    return results
