#!/usr/bin/env python3
"""
admin/app.py — Digital Scout admin UI + Phase 3 auth.

Flask app for:
  - Prompt template management (admin only)
  - Prospect management (admin only)
  - Lead dashboard (any logged-in user)
  - Login / Register / Logout

Run: python -m src.admin.app
"""

import os

import psycopg2
from flask import Flask, jsonify, request, render_template, redirect, url_for, flash
from flask_login import LoginManager, login_required, logout_user, current_user

from src.auth import load_user, verify_login, create_user
from src.lead_store import get_dashboard_leads, get_subscriptions_for_user
from src.prompt_registry import (
    get_all_prompts,
    get_active_prompt,
    upsert_prompt,
    seed_defaults,
    get_connection,
)


app = Flask(__name__)


def _get_db_url():
    return os.environ.get("DATABASE_URL", "postgresql://localhost:5432/digital_scout")


# ── Flask-Login setup ─────────────────────────────────────────────────────────

app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-in-production")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = "Please sign in to access Digital Scout."
login_manager.login_message_category = "warning"


@login_manager.user_loader
def user_loader(user_id):
    return load_user(user_id)


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        user = verify_login(email, password)
        if user:
            from flask_login import login_user
            login_user(user, remember=True)
            next_page = request.args.get("next")
            return redirect(next_page if next_page and next_page.startswith("/") else url_for("index"))
        return render_template("login.html", error="Invalid email or password."), 401

    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        company = request.form.get("company", "")

        if not email or not password:
            return render_template("register.html", error="Email and password are required."), 400

        if len(password) < 8:
            return render_template("register.html", error="Password must be at least 8 characters."), 400

        try:
            user = create_user(email, password, company)
        except ValueError as e:
            return render_template("register.html", error=str(e)), 400

        from flask_login import login_user
        login_user(user, remember=True)
        return redirect(url_for("dashboard"))

    return render_template("register.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


# ── Protected routes ──────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return redirect(url_for("dashboard"))


@app.route("/admin")
@login_required
def admin():
    prompts = get_all_prompts()
    return render_template("index.html", prompts=prompts)


@app.route("/dashboard")
@login_required
def dashboard():
    active_state = request.args.get("state", "").strip().upper()
    active_filter = request.args.get("filter", "").strip()
    query = request.args.get("q", "").strip()
    days = min(int(request.args.get("days", 7)), 30)
    limit = min(int(request.args.get("limit", 100)), 500)

    available_states = ["TX", "NM", "OK", "WY", "ND", "LA"]
    if active_state and active_state not in available_states:
        active_state = ""

    if active_state:
        from src.lead_store import get_recent_leads
        leads = get_recent_leads(state=active_state, days=days, limit=limit, query=query, filter=active_filter)
    else:
        leads = get_dashboard_leads(current_user.id, days=days, limit=limit, query=query, filter=active_filter)

    # Annotate is_new (processed within last 8 hrs) and days_ago
    import datetime
    now_ts = datetime.datetime.now(datetime.timezone.utc)
    for lead in leads:
        processed = lead.get("processed_at")
        if processed:
            if isinstance(processed, str):
                try:
                    processed = datetime.datetime.fromisoformat(processed.replace("Z", "+00:00"))
                except Exception:
                    processed = None
            elif isinstance(processed, datetime.datetime):
                pass
            else:
                processed = None
        lead["is_new"] = False
        lead["days_ago"] = None
        if processed:
            diff = (now_ts - processed).total_seconds()
            lead["is_new"] = diff < 28800  # 8 hours
            lead["days_ago"] = int(diff // 86400)

    return render_template(
        "dashboard.html",
        leads=leads,
        available_states=available_states,
        active_state=active_state,
        active_filter=active_filter,
        days=days,
        query=query,
        now=now_ts,
    )


@app.route("/lead/<api_number>/<state>")
@login_required
def lead_detail(api_number, state):
    from src.lead_store import get_lead_detail, get_correlated_leads
    lead = get_lead_detail(api_number, state)
    if not lead:
        return "Lead not found", 404
    back_url = request.args.get("back", f"/dashboard?state={state}")
    related_leads = []
    if lead.get("correlation_id"):
        related_leads = [
            r for r in get_correlated_leads(lead["correlation_id"])
            if r["api_number"] != api_number or r["state"] != state
        ]
    return render_template("lead_detail.html", lead=lead, back_url=back_url, related_leads=related_leads)


@app.route("/correlated")
@login_required
def correlated():
    from src.lead_store import get_recent_correlations
    correlations = get_recent_correlations(days=7, limit=50)
    return render_template("correlated.html", correlations=correlations)


@app.route("/account", methods=["GET", "POST"])
@login_required
def account():
    from src.lead_store import get_subscriptions_for_user, upsert_subscription, delete_subscription
    all_states = ["TX", "NM", "OK", "WY", "ND", "LA"]
    user_id = current_user.id

    if request.method == "POST":
        action = request.form.get("action")
        state = request.form.get("state", "").upper()
        if action == "subscribe" and state in all_states:
            upsert_subscription(user_id, state)
        elif action == "unsubscribe" and state in all_states:
            delete_subscription(user_id, state)
        return redirect(url_for("account"))

    subscribed = get_subscriptions_for_user(user_id)
    subscribed_set = set(subscribed)
    return render_template("account.html", all_states=all_states, subscribed=subscribed_set)


@app.route("/api/prompts", methods=["GET"])
@login_required
def api_list_prompts():
    state = request.args.get("state")
    prompts = get_all_prompts(state=state)
    return jsonify([
        {
            "id": p.id,
            "state": p.state,
            "template_type": p.template_type,
            "version": p.version,
            "description": p.description,
            "is_active": p.is_active,
        }
        for p in prompts
    ])


@app.route("/api/prompts/<int:prompt_id>", methods=["GET"])
@login_required
def api_get_prompt(prompt_id):
    query = """
        SELECT id, state, template_type, version, system_prompt,
               user_template, description, is_active, created_at
        FROM prompt_templates WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (prompt_id,))
            row = cur.fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "id": row[0], "state": row[1], "template_type": row[2],
        "version": row[3], "system_prompt": row[4], "user_template": row[5],
        "description": row[6], "is_active": row[7],
    })


@app.route("/api/prompts", methods=["POST"])
@login_required
def api_save_prompt():
    data = request.json
    p = upsert_prompt(
        state=data["state"],
        template_type=data["template_type"],
        system_prompt=data["system_prompt"],
        user_template=data["user_template"],
        description=data.get("description"),
    )
    return jsonify({
        "id": p.id, "state": p.state, "template_type": p.template_type,
        "version": p.version, "is_active": p.is_active,
    })


@app.route("/api/prospects", methods=["GET"])
@login_required
def api_list_prospects():
    query = """
        SELECT id, name, products, counties, formations, website, tier, is_active
        FROM prospects WHERE is_active = TRUE ORDER BY tier, name
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return jsonify([
        {
            "id": r[0], "name": r[1], "products": r[2],
            "counties": r[3], "formations": r[4],
            "website": r[5], "tier": r[6], "is_active": r[7],
        }
        for r in rows
    ])


@app.route("/api/prospects", methods=["POST"])
@login_required
def api_save_prospect():
    data = request.json
    query = """
        INSERT INTO prospects (name, products, counties, formations, website, tier)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (
                data["name"], data["products"], data["counties"],
                data["formations"], data.get("website"), data.get("tier", 2),
            ))
            conn.commit()
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
