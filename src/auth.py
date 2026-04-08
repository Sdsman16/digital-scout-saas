"""
auth.py — Flask-Login user management and password utilities.

Self-hosted auth for Digital Scout SaaS.
Uses werkzeug.security for password hashing (no external deps needed).
"""

import os
from dataclasses import dataclass

from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://localhost:5432/digital_scout")


def get_connection():
    return psycopg2.connect(get_db_url())


@dataclass
class User(UserMixin):
    id: int
    email: str
    hashed_password: str
    company: str | None
    is_admin: bool

    def get_id(self) -> str:
        """Flask-Login requires this to return a string."""
        return str(self.id)


def load_user(user_id: int | str) -> User | None:
    """Flask-Login user_loader callback. Returns User or None."""
    query = """
        SELECT id, email, hashed_password, company, is_admin
        FROM users WHERE id = %s
    """
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return None
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (user_id,))
            row = cur.fetchone()
    if not row:
        return None
    return User(
        id=row["id"],
        email=row["email"],
        hashed_password=row["hashed_password"],
        company=row["company"],
        is_admin=row["is_admin"],
    )


def get_user_by_email(email: str) -> dict | None:
    """Fetch a user dict by email (for login verification)."""
    query = """
        SELECT id, email, hashed_password, company, is_admin
        FROM users WHERE email = %s
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (email.lower().strip(),))
            row = cur.fetchone()
    return dict(row) if row else None


def create_user(email: str, password: str, company: str = "") -> User:
    """
    Create a new user. Hashes the password with werkzeug.
    Returns the new User object.
    Raises ValueError if email already exists.
    """
    # Check duplicate
    existing = get_user_by_email(email)
    if existing:
        raise ValueError("Email already registered")

    hashed = generate_password_hash(password)
    query = """
        INSERT INTO users (email, hashed_password, company, is_admin)
        VALUES (%s, %s, %s, FALSE)
        RETURNING id, email, hashed_password, company, is_admin
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (email.lower().strip(), hashed, company))
            row = cur.fetchone()
            conn.commit()
    return User(
        id=row[0],
        email=row[1],
        hashed_password=row[2],
        company=row[3],
        is_admin=row[4],
    )


def verify_login(email: str, password: str) -> User | None:
    """
    Verify email + password. Returns User if valid, None if invalid.
    """
    user_dict = get_user_by_email(email)
    if not user_dict:
        return None
    if not check_password_hash(user_dict["hashed_password"], password):
        return None
    return User(
        id=user_dict["id"],
        email=user_dict["email"],
        hashed_password=user_dict["hashed_password"],
        company=user_dict["company"],
        is_admin=user_dict["is_admin"],
    )


def get_subscriptions_for_user(user_id: int) -> list[str]:
    """
    Return list of state codes the user is subscribed to.
    e.g. ['TX', 'NM', 'LA']
    """
    query = """
        SELECT state FROM subscriptions
        WHERE user_id = %s AND status = 'active'
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id,))
            return [row[0] for row in cur.fetchall()]
