import os
import json
import psycopg2
import sys
from psycopg2.extras import Json
from dotenv import load_dotenv
import logging
from datetime import datetime
sys.path.append('/opt/airflow')
from get_data.minio_client import get_minio_client, download_json, list_objects
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.info("PostgreSQL connection successful")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def _parse_datetime(ts_str: str):
    """Parse ISO timestamp like 2025-08-15T19:00:00Z"""
    if ts_str:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return None

def load_object_to_db(bucket, object_name, conn):
    try:
        data = download_json(get_minio_client(), bucket, object_name)
        if data is None:
            logger.warning(f"Skipping {object_name} due to download/parsing error")
            return

        cur = conn.cursor()
        for m in data.get("matches", []):
            competition =  m.get("competition",{}) or {}
            match_id = m.get("id")
            if not match_id or (competition.get("code") != "PL" and competition.get("id") != 2021):
                logger.debug(f"skip non-PL match {m.get('id')} comp={competition.get('code')}/{competition.get('id')}")
                continue

            # timestamps
            match_date = _parse_datetime(m.get("utcDate"))
            last_updated = _parse_datetime(m.get("lastUpdated"))

            # competition/season/area
            competition = m.get("competition") or {}
            competition_id = competition.get("id")
            season = m.get("season") or {}
            season_id = season.get("id")
            matchday = m.get("matchday")
            stage = m.get("stage")
            area = m.get("area") or {}
            area_id = area.get("id")
            area_name = area.get("name")

            # teams
            home = m.get("homeTeam") or {}
            away = m.get("awayTeam") or {}
            home_id = home.get("id")
            away_id = away.get("id")

            # upsert teams
            if home_id:
                cur.execute("""
                    INSERT INTO dev.silver_PL_team (team_id, team_name, short_name, tla, crest)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (team_id) DO UPDATE
                    SET team_name = EXCLUDED.team_name,
                        short_name = COALESCE(EXCLUDED.short_name, dev.silver_PL_team.short_name),
                        tla = COALESCE(EXCLUDED.tla, dev.silver_PL_team.tla),
                        crest = COALESCE(EXCLUDED.crest, dev.silver_PL_team.crest);
                """, (
                    home_id, home.get("name"), home.get("shortName"), home.get("tla"), home.get("crest")
                ))
            if away_id:
                cur.execute("""
                    INSERT INTO dev.silver_PL_team (team_id, team_name, short_name, tla, crest)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (team_id) DO UPDATE
                    SET team_name = EXCLUDED.team_name,
                        short_name = COALESCE(EXCLUDED.short_name, dev.silver_PL_team.short_name),
                        tla = COALESCE(EXCLUDED.tla, dev.silver_PL_team.tla),
                        crest = COALESCE(EXCLUDED.crest, dev.silver_PL_team.crest);
                """, (
                    away_id, away.get("name"), away.get("shortName"), away.get("tla"), away.get("crest")
                ))

            # scores
            score = m.get("score") or {}
            full_time = score.get("fullTime") or {}
            half_time = score.get("halfTime") or {}
            extra_time = score.get("extraTime") or {}
            penalties = score.get("penalties") or {}

            # referees
            referees = m.get("referees") or []
            primary_ref = referees[0] if referees else {}
            ref_id = primary_ref.get("id")
            ref_name = primary_ref.get("name")
            ref_nat = primary_ref.get("nationality")

            # Upsert match
            cur.execute("""
                INSERT INTO dev.silver_PL_matches
                (match_id, competition_id, season_id, matchday, stage, area_id, area_name,
                 match_date, last_updated, match_status, winner, duration,
                 full_time_home, full_time_away, half_time_home, half_time_away,
                 extra_time_home, extra_time_away, penalties_home, penalties_away,
                 home_team_id, away_team_id,
                 referee_primary_id, referee_primary_name, referee_primary_nationality,
                 referees, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, NOW())
                ON CONFLICT (match_id) DO UPDATE
                SET competition_id = EXCLUDED.competition_id,
                    season_id = EXCLUDED.season_id,
                    matchday = EXCLUDED.matchday,
                    stage = EXCLUDED.stage,
                    area_id = EXCLUDED.area_id,
                    area_name = EXCLUDED.area_name,
                    match_date = EXCLUDED.match_date,
                    last_updated = EXCLUDED.last_updated,
                    match_status = EXCLUDED.match_status,
                    winner = EXCLUDED.winner,
                    duration = EXCLUDED.duration,
                    full_time_home = EXCLUDED.full_time_home,
                    full_time_away = EXCLUDED.full_time_away,
                    half_time_home = EXCLUDED.half_time_home,
                    half_time_away = EXCLUDED.half_time_away,
                    extra_time_home = EXCLUDED.extra_time_home,
                    extra_time_away = EXCLUDED.extra_time_away,
                    penalties_home = EXCLUDED.penalties_home,
                    penalties_away = EXCLUDED.penalties_away,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    referee_primary_id = EXCLUDED.referee_primary_id,
                    referee_primary_name = EXCLUDED.referee_primary_name,
                    referee_primary_nationality = EXCLUDED.referee_primary_nationality,
                    referees = EXCLUDED.referees,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW();
            """, (
                match_id, competition_id, season_id, matchday, stage, area_id, area_name,
                match_date, last_updated, m.get("status"), score.get("winner"), score.get("duration"),
                full_time.get("home"), full_time.get("away"), half_time.get("home"), half_time.get("away"),
                extra_time.get("home"), extra_time.get("away"), penalties.get("home"), penalties.get("away"),
                home_id, away_id,
                ref_id, ref_name, ref_nat,
                Json(referees), Json(m)
            ))

        conn.commit()
        cur.close()
        logger.info(f"Inserted/updated PL matches from {object_name}")
    except psycopg2.Error as e:
        logger.error(f"Database error for {object_name}: {e}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error for {object_name}: {e}")

def run_load(bucket="football", prefix="bronze/matches"):
    logger.info(f"Starting run_load with bucket={bucket}, prefix={prefix}")
    try:
        conn = connect_db()

        objects = list_objects(get_minio_client(), bucket, prefix)
        if not objects:
            logger.warning(f"No objects found in {bucket}/{prefix}")
            return

        for object_name in objects:
            try:
                logger.info(f"Processing {object_name}")
                load_object_to_db(bucket, object_name, conn)
            except Exception as e:
                logger.error(f"Failed to process {object_name}: {e}")
                continue
    except Exception as e:
        logger.error(f"Error in run_load: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    run_load()
