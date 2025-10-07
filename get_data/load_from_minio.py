import os
import json
import psycopg2
from psycopg2.extras import Json
from minio_client import get_minio_client, download_json, list_objects
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    """Connect to PostgreSQL football_db"""
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ.get("POSTGRES_DB", "football_db"),
            user=os.environ.get("POSTGRES_USER", "admin"),
            password=os.environ.get("POSTGRES_PASSWORD", "1234")
        )
        print("PostgreSQL connection successful")
        return conn
    except psycopg2.Error as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        raise

def create_table(conn):
    """Create schema + table if not exists"""
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_matches (
                id SERIAL PRIMARY KEY,
                match_id BIGINT UNIQUE NOT NULL,
                competition_name TEXT,
                home_team_name TEXT,
                away_team_name TEXT,
                match_date TIMESTAMP,
                match_status TEXT,
                raw_json JSONB,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        print("Table dev.raw_matches ready")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise

def load_object_to_db(bucket, object_name, conn):
    """Load one object from MinIO into PostgreSQL"""
    try:
        data = download_json(get_minio_client(), bucket, object_name)
        if data is None:
            print(f"⚠Skipping {object_name} due to download/parsing error")
            return

        cur = conn.cursor()
        for m in data.get("matches", []):
            match_date = m.get("utcDate")
            if match_date is None:
                print(f"⚠Skipping match {m.get('id')} (null utcDate)")
                continue

            cur.execute("""
                INSERT INTO dev.raw_matches
                (match_id, competition_name, home_team_name, away_team_name, match_date, match_status, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_id) DO NOTHING;
            """, (
                m.get("id"),
                m.get("competition", {}).get("name"),
                m.get("homeTeam", {}).get("name"),
                m.get("awayTeam", {}).get("name"),
                match_date,
                m.get("status"),
                Json(m)
            ))
        conn.commit()
        cur.close()
        print(f"Inserted data from {object_name}")
    except psycopg2.Error as e:
        print(f"Database error for {object_name}: {e}")
        conn.rollback()

def run_load(bucket="football", prefix="bronze/matches"):
    print(f" Starting run_load with bucket={bucket}, prefix={prefix}")
    try:
        conn = connect_db()
        create_table(conn)

        objects = list_objects(get_minio_client(), bucket, prefix)
        if not objects:
            print(f"No objects found in {bucket}/{prefix}")
            return

        for object_name in objects:
            try:
                print(f" Processing {object_name}")
                load_object_to_db(bucket, object_name, conn)
            except Exception as e:
                print(f"Failed to process {object_name}: {e}")
                continue
    except Exception as e:
        print(f"Error in run_load: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    run_load()
