-- Digital Scout SaaS — Initial Schema
-- Run: psql $DATABASE_URL -f migrations/001_initial.sql

-- ── Prompt Registry ───────────────────────────────────────────────────────────
CREATE TABLE prompt_templates (
    id          SERIAL PRIMARY KEY,
    state       VARCHAR(10) NOT NULL,       -- TX, NM, OK, WY, ND, LA, or 'ALL'
    template_type VARCHAR(20) NOT NULL,     -- brief, alert, summary, correlated
    version     INTEGER NOT NULL DEFAULT 1,
    system_prompt TEXT NOT NULL,
    user_template TEXT NOT NULL,
    description TEXT,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(state, template_type, version)
);

CREATE INDEX idx_prompt_state_type ON prompt_templates(state, template_type) WHERE is_active = TRUE;

-- ── Leads ────────────────────────────────────────────────────────────────────
CREATE TABLE leads (
    id              SERIAL PRIMARY KEY,
    api_number      VARCHAR(20) NOT NULL,
    state           VARCHAR(10) NOT NULL,
    operator_name   VARCHAR(255),
    well_name       VARCHAR(255),
    county          VARCHAR(100),
    well_type       VARCHAR(50),
    status          VARCHAR(50),
    latitude        FLOAT,
    longitude       FLOAT,
    total_depth_ft  INTEGER,
    formation       VARCHAR(100),
    spud_date       DATE,
    is_high_pressure    BOOLEAN DEFAULT FALSE,
    has_h2s_risk       BOOLEAN DEFAULT FALSE,
    is_directional      BOOLEAN DEFAULT FALSE,
    is_pre_spud        BOOLEAN DEFAULT FALSE,
    raw_data        JSONB,                  -- full raw permit payload
    correlation_id  INTEGER,                -- FK to correlations table
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(api_number, state)
);

CREATE INDEX idx_leads_state_date    ON leads(state, processed_at DESC);
CREATE INDEX idx_leads_operator      ON leads(operator_name varchar_pattern_ops);
CREATE INDEX idx_leads_correlation   ON leads(correlation_id) WHERE correlation_id IS NOT NULL;

-- ── Correlated Lead Groups ───────────────────────────────────────────────────
CREATE TABLE correlations (
    id              SERIAL PRIMARY KEY,
    correlation_key VARCHAR(255) NOT NULL,   -- normalized operator + week hash
    operator_name   VARCHAR(255) NOT NULL,
    week_number     INTEGER NOT NULL,        -- ISO week of processed_at
    year            INTEGER NOT NULL,
    states          VARCHAR(50)[] NOT NULL, -- ['TX','NM','OK'] etc.
    lead_count      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_corr_key    ON correlations(correlation_key);
CREATE INDEX idx_corr_week   ON correlations(year, week_number);

-- ── Supplier Prospects ────────────────────────────────────────────────────────
CREATE TABLE prospects (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    products    TEXT[] NOT NULL,
    counties    TEXT[] NOT NULL,
    formations  TEXT[] NOT NULL,
    website     VARCHAR(255),
    tier        SMALLINT NOT NULL DEFAULT 2,  -- 1=primary, 2=secondary, 3=safety
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prospects_tier ON prospects(tier) WHERE is_active = TRUE;

-- ── Lead-to-Prospect Matches ─────────────────────────────────────────────────
CREATE TABLE matches (
    id              SERIAL PRIMARY KEY,
    lead_id         INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    prospect_id     INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
    score           FLOAT NOT NULL,
    score_breakdown JSONB,                      -- {county: 3, formation: 3, h2s: 2, ...}
    rank            SMALLINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(lead_id, prospect_id)
);

CREATE INDEX idx_matches_lead    ON matches(lead_id, score DESC);

-- ── Generated Briefs ─────────────────────────────────────────────────────────
CREATE TABLE briefs (
    id          SERIAL PRIMARY KEY,
    lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    brief_text  TEXT NOT NULL,
    model_used  VARCHAR(100),
    token_count INTEGER,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_briefs_lead ON briefs(lead_id);

-- ── Users ────────────────────────────────────────────────────────────────────
CREATE TABLE users (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(255) UNIQUE NOT NULL,
    hashed_password  VARCHAR(255) NOT NULL,
    company         VARCHAR(255),
    slack_user_id   VARCHAR(100),
    slack_workspace VARCHAR(100),
    is_admin        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Subscriptions ─────────────────────────────────────────────────────────────
CREATE TABLE subscriptions (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    state           VARCHAR(10) NOT NULL,
    tier            VARCHAR(20) NOT NULL DEFAULT 'basic',  -- free, basic, pro
    stripe_sub_id   VARCHAR(255),                           -- Stripe subscription ID
    status          VARCHAR(20) NOT NULL DEFAULT 'active', -- active, cancelled, past_due
    current_period_end DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, state)
);

CREATE INDEX idx_subs_user    ON subscriptions(user_id);
CREATE INDEX idx_subs_status ON subscriptions(status) WHERE status = 'active';

-- ── Delivery Log ─────────────────────────────────────────────────────────────
CREATE TABLE deliveries (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    lead_id         INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    correlation_id  INTEGER REFERENCES correlations(id) ON DELETE SET NULL,
    channel         VARCHAR(50) NOT NULL,   -- slack, email, webhook
    delivered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          VARCHAR(20) NOT NULL DEFAULT 'delivered', -- delivered, failed, skipped
    error_message   TEXT
);

CREATE INDEX idx_deliveries_user   ON deliveries(user_id, delivered_at DESC);
CREATE INDEX idx_deliveries_lead   ON deliveries(lead_id) WHERE lead_id IS NOT NULL;
