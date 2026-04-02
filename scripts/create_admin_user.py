"""
create_admin_user.py
====================
CLI tool to create or reset dashboard users.
Run this ONCE after applying 06_rbac_auth.sql to set up your first admin.

Usage:
  python create_admin_user.py --username admin --name "Admin" --role admin
  python create_admin_user.py --username artuha --name "Artuha" --role user
  python create_admin_user.py --reset-password --username admin
  python create_admin_user.py --list

Requirements: pip install bcrypt psycopg2-binary python-dotenv
"""

import argparse
import getpass
import os
import sys
from pathlib import Path

import bcrypt
import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "crm_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def create_user(username: str, name: str, role: str) -> None:
    password = getpass.getpass(f"Set password for '{username}': ")
    confirm  = getpass.getpass("Confirm password: ")

    if password != confirm:
        print("ERROR: Passwords do not match.")
        sys.exit(1)
    if len(password) < 6:
        print("ERROR: Password must be at least 6 characters.")
        sys.exit(1)

    hashed = hash_password(password)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dashboard_users (username, name, password_hash, role)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE
                    SET name          = EXCLUDED.name,
                        password_hash = EXCLUDED.password_hash,
                        role          = EXCLUDED.role,
                        is_active     = TRUE
            """, (username.lower(), name, hashed, role))
        conn.commit()
        print(f"User '{username}' ({role}) created/updated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        conn.close()


def reset_password(username: str) -> None:
    password = getpass.getpass(f"New password for '{username}': ")
    confirm  = getpass.getpass("Confirm password: ")

    if password != confirm:
        print("ERROR: Passwords do not match.")
        sys.exit(1)
    if len(password) < 6:
        print("ERROR: Password must be at least 6 characters.")
        sys.exit(1)

    hashed = hash_password(password)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dashboard_users
                SET password_hash = %s
                WHERE username = %s
            """, (hashed, username.lower()))
            if cur.rowcount == 0:
                print(f"ERROR: User '{username}' not found.")
                sys.exit(1)
        conn.commit()
        print(f"Password for '{username}' reset successfully.")
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        conn.close()


def deactivate_user(username: str) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dashboard_users
                SET is_active = FALSE
                WHERE username = %s
            """, (username.lower(),))
            if cur.rowcount == 0:
                print(f"ERROR: User '{username}' not found.")
                sys.exit(1)
        conn.commit()
        print(f"User '{username}' deactivated.")
    finally:
        conn.close()


def list_users() -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT username, name, role, is_active, last_login
                FROM dashboard_users
                ORDER BY role, username
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("No users found. Run with --username to create one.")
        return

    print(f"\n{'USERNAME':<20} {'NAME':<25} {'ROLE':<10} {'ACTIVE':<8} {'LAST LOGIN'}")
    print("-" * 80)
    for row in rows:
        username, name, role, active, last_login = row
        login_str = last_login.strftime("%d/%m/%Y %H:%M") if last_login else "Never"
        print(f"{username:<20} {name:<25} {role:<10} {str(active):<8} {login_str}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage CRM dashboard users")
    parser.add_argument("--username",       type=str, help="Username (lowercase)")
    parser.add_argument("--name",           type=str, help="Display name")
    parser.add_argument("--role",           type=str, choices=["admin", "user"], default="user")
    parser.add_argument("--reset-password", action="store_true", help="Reset password for existing user")
    parser.add_argument("--deactivate",     action="store_true", help="Deactivate a user")
    parser.add_argument("--list",           action="store_true", help="List all users")
    args = parser.parse_args()

    if args.list:
        list_users()
    elif args.deactivate:
        if not args.username:
            parser.error("--deactivate requires --username")
        deactivate_user(args.username)
    elif args.reset_password:
        if not args.username:
            parser.error("--reset-password requires --username")
        reset_password(args.username)
    elif args.username and args.name:
        create_user(args.username, args.name, args.role)
    else:
        parser.print_help()
