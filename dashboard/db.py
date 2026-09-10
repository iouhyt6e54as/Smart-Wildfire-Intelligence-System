import os
import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "wildfire"),
        user=os.getenv("DB_USER", "wildfire"),
        password=os.getenv("DB_PASSWORD", "wildfire_secret"),
    )


if __name__ == "__main__":
    try:
        conn = get_connection()
        print("PostgreSQL connection: OK")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user;")
            result = cur.fetchone()
            print(f"Database: {result[0]}")
            print(f"User: {result[1]}")

        conn.close()

    except Exception as e:
        print("PostgreSQL connection: FAILED")
        print(e)
