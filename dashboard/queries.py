import pandas as pd

from db import get_connection


def get_fire_events(limit=10000):
    query = """
        SELECT
            event_timestamp,
            latitude,
            longitude,
            brightness,
            bright_t31,
            scan,
            track,
            frp,
            confidence,
            daynight,
            satellite,
            instrument,
            version,
            type,
            year,
            season,
            is_night
        FROM wildfire.fire_events
        ORDER BY event_timestamp DESC
        LIMIT %s;
    """

    conn = get_connection()

    try:
        return pd.read_sql_query(query, conn, params=(limit,))
    finally:
        conn.close()


def get_fire_kpis():
    query = """
        SELECT
            COUNT(*) AS total_fires,
            COALESCE(SUM(frp), 0) AS total_frp,
            COALESCE(AVG(frp), 0) AS avg_frp,
            COUNT(*) FILTER (WHERE is_night = 1) AS night_fires
        FROM wildfire.fire_events;
    """

    conn = get_connection()

    try:
        return pd.read_sql_query(query, conn).iloc[0]
    finally:
        conn.close()


def get_daily_fire_kpis():
    query = """
        SELECT
            event_date,
            fire_count,
            avg_frp,
            max_frp,
            night_fires
        FROM wildfire.v_daily_fire_kpis
        ORDER BY event_date;
    """

    conn = get_connection()

    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()
