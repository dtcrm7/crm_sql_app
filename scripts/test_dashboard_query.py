import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def check_dashboard_query(campaign="consulting"):
    conn = psycopg2.connect(**DB_CONFIG)
    
    # This is the EXACT query from 1_Pipeline.py
    sql = """
        WITH bd_counts AS (
            SELECT
                contact_id,
                COUNT(*)                                            AS total_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Connected')   AS connected_attempts,
                COUNT(*) FILTER (WHERE current_state = 'Interested') AS interested_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Invalid Number') AS invalid_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Do not Disturb') AS dnd_attempts,
                COUNT(*) FILTER (WHERE current_state IN ('Not interested', 'Not Interested')) AS not_int_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Referred' OR current_state IN ('Referred', 'Reffered')) AS referred_attempts,
                COUNT(*) FILTER (WHERE current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months')) AS realloc_3m_attempts,
                COUNT(*) FILTER (WHERE current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months') AND called_at <= NOW() - INTERVAL '90 days') AS realloc_3m_ready_attempts,
                MIN(called_at)                                      AS first_called_at,
                MIN(called_at) FILTER (WHERE current_state = 'Shared Story') AS first_story_at,
                MIN(called_at) FILTER (WHERE current_state IN ('Snapshot Sent', 'Dream Snapshot Sent')) AS first_snapshot_at
            FROM call_actions
            GROUP BY contact_id
        ),
        mql_counts AS (
            SELECT
                mca.contact_id,
                COUNT(*) FILTER (WHERE mca.call_status = 'Invalid Number') AS invalid_attempts,
                COUNT(*) FILTER (WHERE mca.call_status = 'Do not Disturb') AS dnd_attempts,
                COUNT(*) FILTER (WHERE mca.current_state = 'Not interested') AS not_int_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Referred', 'Reffered')) AS referred_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months')) AS realloc_3m_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months') AND mca.called_at <= NOW() - INTERVAL '90 days') AS realloc_3m_ready_attempts
            FROM mql_call_attempts mca
            JOIN mql_allocations ma ON ma.id = mca.allocation_id
            JOIN agents am ON am.id = mca.agent_id
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason IS DISTINCT FROM 'bd_history'
              AND am.team = 'mql'
            GROUP BY mca.contact_id
        )
        SELECT
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.first_story_at IS NOT NULL 
                   OR c.contact_flag IN ('shared_story', 'snapshot_sent', 'bd_qualified', 'mql_qualified', 'meeting_in_progress', 'mql_rejected')
            )                                                               AS shared_story,
            COUNT(DISTINCT c.id) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM mql_call_attempts mca 
                    WHERE mca.contact_id = c.id 
                      AND mca.current_state IN ('Snapshot Confirmed', 'Dream Snapshot Confirmed')
                )
            )                                                               AS true_mql
        FROM contacts c
        LEFT JOIN bd_counts bc ON bc.contact_id = c.id
        LEFT JOIN mql_counts mc ON mc.contact_id = c.id
        WHERE c.campaign = %(campaign)s
    """
    
    params = {"campaign": campaign, "campaign_like": f"{campaign} %"}
    
    df = pd.read_sql(sql, conn, params=params)
    print(df)
    conn.close()

if __name__ == "__main__":
    check_dashboard_query()
