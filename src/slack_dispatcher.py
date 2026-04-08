"""
slack_dispatcher.py — Build Block Kit messages and dispatch to Slack with retries.

Phase 2: Handles deduplication, exponential backoff retry, and
correlated multi-state alerts.
"""

import os
import time
import logging
from dataclasses import dataclass
from datetime import datetime

import requests

log = logging.getLogger(__name__)


# ── Block Kit Builders ─────────────────────────────────────────────────────────

def build_lead_blocks(
    lead: dict,
    brief: str | None,
    matched_prospects: list[dict] | None = None,
    starred: bool = False,
) -> list[dict]:
    """Build a Slack Block Kit message for a single lead."""
    star = "🥇 " if starred else ""
    county = lead.get("county") or "Unknown"
    state = lead.get("state") or "??"
    api = lead.get("api_number") or lead.get("api") or ""
    operator = lead.get("operator_name") or lead.get("operator") or "Unknown"
    formation = lead.get("formation") or "N/A"
    depth = lead.get("total_depth_ft") or lead.get("td") or 0
    depth_str = f"{depth:,}" if depth else "N/A"
    spud = lead.get("spud_date") or "N/A"
    hp = lead.get("is_high_pressure")
    h2s = lead.get("has_h2s_risk")
    dir_ = lead.get("is_directional")

    hp_icon = " ⚠️ HIGH PRESSURE" if hp else ""
    h2s_icon = " ⚠️ H2S" if h2s else ""
    dir_icon = " ↗️ DIRECTIONAL" if dir_ else ""

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{star}🌟 {state} Lead: {county} County{hp_icon}{h2s_icon}{dir_icon}",
                "emoji": True,
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*API:* `{api}`",
            }
        },
        {"type": "divider"},
    ]

    if brief:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": brief},
        })

    if matched_prospects:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🎯 Matched Suppliers:*"},
        })
        for p in matched_prospects:
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(p.get("tier", 99), "•")
            name = p.get("name", "")
            products = ", ".join((p.get("products") or [])[:3])
            website = p.get("website") or ""
            web_line = f" | <https://{website}|{website}>" if website else ""
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{emoji} *{name}*\n_{products}_\n{web_line}",
                }
            })
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "_Supplier match based on county, formation, and well attributes._",
            }]
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"_Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} "
                f"by Digital Scout — {state} Permit Intelligence_"
            )
        }]
    })
    blocks.append({"type": "divider"})

    return blocks


def build_correlated_blocks(
    group: "CorrelatedGroup",
    brief: str | None,
    matched_prospects: list[dict] | None = None,
    starred: bool = False,
) -> list[dict]:
    """Build a Slack Block Kit message for a correlated multi-state alert."""
    star = "🥇 " if starred else ""
    states = ", ".join(group.states)
    operator = group.operator_name
    year, week = group.week
    corr_key = f"{operator}:{year}:W{week:02d}"

    emoji_by_state = {
        "TX": "TX", "NM": "NM", "OK": "OK",
        "WY": "WY", "ND": "ND", "LA": "LA",
    }

    state_bar = " ".join(
        f"*{emoji_by_state.get(s, s)}*" for s in group.states
    )

    # Build per-state summary lines
    by_state = {}
    for lead in group.leads:
        st = lead.get("state", "?")
        if st not in by_state:
            by_state[st] = []
        by_state[st].append(lead)

    state_lines = []
    for st in group.states:
        leads_st = by_state.get(st, [])
        counties = sorted(set(l.get("county") or "?" for l in leads_st))
        formations = sorted(set(l.get("formation") or "?" for l in leads_st if l.get("formation")))
        depth = max((l.get("total_depth_ft") or 0) for l in leads_st)
        hp = any(l.get("is_high_pressure") for l in leads_st)
        h2s = any(l.get("has_h2s_risk") for l in leads_st)

        line = (
            f"• *{st}*: {len(leads_st)} lead(s) — "
            f"{', '.join(counties[:3])}{' +more' if len(counties) > 3 else ''} | "
            f"{', '.join(formations[:2]) or '?'} | "
            f"{f'{depth:,} ft' if depth else '?'}"
            f"{' | ⚠️ HP' if hp else ''}"
            f"{' | ⚠️ H2S' if h2s else ''}"
        )
        state_lines.append(line)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{star}🔗 Correlated Alert: {operator}",
                "emoji": True,
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*States:* {state_bar}   "
                    f"*Week {year}-W{week:02d}*   "
                    f"*Total: {group.lead_count} leads*"
                ),
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(state_lines),
            }
        },
    ]

    if brief:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": brief},
        })

    if matched_prospects:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🎯 Matched Suppliers:*"},
        })
        for p in matched_prospects:
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(p.get("tier", 99), "•")
            products = ", ".join((p.get("products") or [])[:3])
            website = p.get("website") or ""
            web_line = f" | <https://{website}|{website}>" if website else ""
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{emoji} *{p['name']}*\n_{products}_\n{web_line}",
                }
            })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"_Correlated by Digital Scout | {states} | "
                f"Week {year}-W{week:02d}_"
            )
        }]
    })
    blocks.append({"type": "divider"})

    return blocks


# ── Dispatcher ────────────────────────────────────────────────────────────────

@dataclass
class DeliveryResult:
    success: bool
    status_code: int | None
    error: str | None
    attempt: int


class SlackDispatcher:
    """
    Dispatch messages to Slack with deduplication and exponential backoff.

    deduplication_key: a string that, if recently sent within the TTL window,
    skips the request entirely. E.g. f"{state}:{api_number}" for single leads,
    or f"corr:{correlation_key}" for correlated alerts.
    """

    MAX_RETRIES = 3
    BASE_DELAY_SECS = 5
    DEDUP_TTL_SECS = 60 * 60 * 6  # 6 hours

    def __init__(self, webhook_url: str | None = None):
        import yaml
        self._webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_DEFAULT", "")
        self._dedup: dict[str, float] = {}

    def _is_duplicate(self, dedup_key: str) -> bool:
        """Check if a dedup key was sent recently."""
        import time
        if dedup_key in self._dedup:
            if time.time() - self._dedup[dedup_key] < self.DEDUP_TTL_SECS:
                return True
        return False

    def _mark_sent(self, dedup_key: str) -> None:
        import time
        self._dedup[dedup_key] = time.time()

    def dispatch(
        self,
        blocks: list[dict],
        webhook_url: str | None = None,
        dedup_key: str | None = None,
        text_fallback: str | None = None,
    ) -> DeliveryResult:
        """
        POST blocks to Slack with retry + exponential backoff.

        Returns DeliveryResult with success status, status code, and error.
        """
        url = webhook_url or self._webhook_url
        if not url:
            return DeliveryResult(False, None, "No webhook URL configured")

        if dedup_key and self._is_duplicate(dedup_key):
            log.info(f"Deduplicated: {dedup_key}")
            return DeliveryResult(True, 200, None)

        payload = {"blocks": blocks}
        if text_fallback:
            payload["text"] = text_fallback

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = requests.post(url, json=payload, timeout=30)
                if resp.status_code == 200:
                    self._mark_sent(dedup_key)
                    return DeliveryResult(True, 200, None)
                if resp.status_code == 429:
                    # Rate limited — back off
                    retry_after = int(resp.headers.get("Retry-After", self.BASE_DELAY_SECS * attempt * 2))
                    log.warning(f"Slack rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                # Other error — retry
                log.warning(f"Slack returned {resp.status_code}: {resp.text[:100]}")
            except requests.exceptions.Timeout:
                log.warning(f"Slack timeout, attempt {attempt}/{self.MAX_RETRIES}")
            except requests.exceptions.RequestException as e:
                log.warning(f"Slack request error: {e}")

            if attempt < self.MAX_RETRIES:
                delay = self.BASE_DELAY_SECS * (2 ** (attempt - 1))
                log.info(f"Retrying in {delay}s...")
                time.sleep(delay)

        return DeliveryResult(False, None, "All retry attempts failed")

    def dispatch_lead(
        self,
        lead: dict,
        brief: str | None = None,
        matched_prospects: list[dict] | None = None,
        starred: bool = False,
        webhook_url: str | None = None,
    ) -> DeliveryResult:
        """Convenience: build and dispatch a single lead alert."""
        blocks = build_lead_blocks(lead, brief, matched_prospects, starred)
        dedup_key = f"{lead.get('state')}:{lead.get('api_number') or lead.get('api')}"
        fallback = (
            f"{lead.get('state')} Lead: {lead.get('county')} County | "
            f"API {lead.get('api_number') or lead.get('api')} | "
            f"{lead.get('operator_name') or lead.get('operator')}"
        )
        return self.dispatch(blocks, webhook_url, dedup_key, fallback)

    def dispatch_correlated(
        self,
        group: "CorrelatedGroup",
        brief: str | None = None,
        matched_prospects: list[dict] | None = None,
        starred: bool = False,
        webhook_url: str | None = None,
    ) -> DeliveryResult:
        """Convenience: build and dispatch a correlated multi-state alert."""
        blocks = build_correlated_blocks(group, brief, matched_prospects, starred)
        dedup_key = f"corr:{group.correlation_key}"
        fallback = (
            f"🔗 Correlated Alert: {group.operator_name} | "
            f"{' + '.join(group.states)} | "
            f"{group.lead_count} leads"
        )
        return self.dispatch(blocks, webhook_url, dedup_key, fallback)
