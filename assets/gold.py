import os
import psycopg2
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_db():
    """Connect to PostgreSQL football_db"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "de_psql"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "football_db"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "1234")
        )
        logger.info("PostgreSQL connection successful")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def transform_to_gold(conn):
    """Aggregate silver data into gold layer for FINISHED matches"""
    try:
        cur = conn.cursor()

        # Log silver data counts
        cur.execute("SELECT COUNT(*) FROM dev.silver_PL_team")
        team_count = cur.fetchone()[0]
        logger.info(f"Found {team_count} teams in silver_PL_team")

        cur.execute("SELECT COUNT(*) FROM dev.silver_PL_matches WHERE match_status = 'FINISHED'")
        finished_count = cur.fetchone()[0]
        logger.info(f"Found {finished_count} FINISHED matches in silver_PL_matches")

        cur.execute("SELECT competition_id, COUNT(*) FROM dev.silver_PL_matches GROUP BY competition_id")
        comp_counts = cur.fetchall()
        for cid, count in comp_counts:
            logger.info(f"Competition ID {cid}: {count} matches")

        # GOLD TEAM STATS
        cur.execute("""
            DELETE FROM dev.gold_team_stats;
            INSERT INTO dev.gold_team_stats
            (team_id, team_name, matches_played, wins, losses, draws,
             total_goals_scored, total_goals_conceded, goal_difference,
             avg_goals_scored, avg_goals_conceded)
            SELECT
                t.team_id,
                t.team_name,
                COUNT(m.match_id) FILTER (WHERE m.match_status = 'FINISHED') AS matches_played,
                COALESCE(SUM(CASE 
                    WHEN m.home_team_id = t.team_id AND COALESCE(m.full_time_home, 0) > COALESCE(m.full_time_away, 0) THEN 1
                    WHEN m.away_team_id = t.team_id AND COALESCE(m.full_time_away, 0) > COALESCE(m.full_time_home, 0) THEN 1
                    ELSE 0 
                END), 0) AS wins,
                COALESCE(SUM(CASE 
                    WHEN m.home_team_id = t.team_id AND COALESCE(m.full_time_home, 0) < COALESCE(m.full_time_away, 0) THEN 1
                    WHEN m.away_team_id = t.team_id AND COALESCE(m.full_time_away, 0) < COALESCE(m.full_time_home, 0) THEN 1
                    ELSE 0 
                END), 0) AS losses,
                COALESCE(SUM(CASE 
                    WHEN m.match_status = 'FINISHED' AND COALESCE(m.full_time_home, 0) = COALESCE(m.full_time_away, 0) THEN 1 
                    ELSE 0 
                END), 0) AS draws,
                COALESCE(SUM(CASE 
                    WHEN m.home_team_id = t.team_id THEN COALESCE(m.full_time_home, 0) 
                    ELSE COALESCE(m.full_time_away, 0) 
                END), 0) AS total_goals_scored,
                COALESCE(SUM(CASE 
                    WHEN m.home_team_id = t.team_id THEN COALESCE(m.full_time_away, 0) 
                    ELSE COALESCE(m.full_time_home, 0) 
                END), 0) AS total_goals_conceded,
                COALESCE(SUM(CASE 
                    WHEN m.home_team_id = t.team_id THEN COALESCE(m.full_time_home, 0) - COALESCE(m.full_time_away, 0)
                    ELSE COALESCE(m.full_time_away, 0) - COALESCE(m.full_time_home, 0) 
                END), 0) AS goal_difference,
                COALESCE(AVG(CASE 
                    WHEN m.home_team_id = t.team_id THEN COALESCE(m.full_time_home, 0)::FLOAT 
                    ELSE COALESCE(m.full_time_away, 0)::FLOAT 
                END), 0) AS avg_goals_scored,
                COALESCE(AVG(CASE 
                    WHEN m.home_team_id = t.team_id THEN COALESCE(m.full_time_away, 0)::FLOAT 
                    ELSE COALESCE(m.full_time_home, 0)::FLOAT 
                END), 0) AS avg_goals_conceded
            FROM dev.silver_PL_team t
            LEFT JOIN dev.silver_PL_matches m 
                ON t.team_id IN (m.home_team_id, m.away_team_id)
                AND m.match_status = 'FINISHED'
            GROUP BY t.team_id, t.team_name;
        """)
        cur.execute("SELECT COUNT(*) FROM dev.gold_team_stats")
        team_stats_count = cur.fetchone()[0]
        logger.info(f"Inserted {team_stats_count} rows into gold_team_stats")

        # GOLD LEAGUE STANDINGS
        cur.execute("""
            DELETE FROM dev.gold_league_standings;
            INSERT INTO dev.gold_league_standings
            (competition_id, team_id, team_name, rank, points, goal_difference, matches_played)
            SELECT
                2021 AS competition_id,
                team_id,
                team_name,
                ROW_NUMBER() OVER (
                    ORDER BY points DESC, goal_difference DESC, team_name ASC
                ) AS rank,
                points,
                goal_difference,
                matches_played
            FROM (
                SELECT
                    t.team_id,
                    t.team_name,
                    COUNT(m.match_id) AS matches_played,
                    COALESCE(SUM(CASE 
                        WHEN (m.home_team_id = t.team_id AND COALESCE(m.full_time_home, 0) > COALESCE(m.full_time_away, 0)) OR
                             (m.away_team_id = t.team_id AND COALESCE(m.full_time_away, 0) > COALESCE(m.full_time_home, 0)) THEN 3
                        WHEN COALESCE(m.full_time_home, 0) = COALESCE(m.full_time_away, 0) THEN 1
                        ELSE 0 
                    END), 0) AS points,
                    COALESCE(SUM(CASE 
                        WHEN m.home_team_id = t.team_id THEN COALESCE(m.full_time_home, 0) - COALESCE(m.full_time_away, 0)
                        ELSE COALESCE(m.full_time_away, 0) - COALESCE(m.full_time_home, 0) 
                    END), 0) AS goal_difference
                FROM dev.silver_PL_team t
                LEFT JOIN dev.silver_PL_matches m 
                    ON t.team_id IN (m.home_team_id, m.away_team_id)
                    AND m.match_status = 'FINISHED'
                GROUP BY t.team_id, t.team_name
            ) s;
        """)
        cur.execute("SELECT COUNT(*) FROM dev.gold_league_standings")
        standings_count = cur.fetchone()[0]
        logger.info(f"Inserted {standings_count} rows into gold_league_standings")

        conn.commit()
        logger.info("Gold layer refreshed successfully")
    except psycopg2.Error as e:
        logger.error(f"Error during gold transformation: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def run_gold():
    """Run gold layer transformation"""
    logger.info("Starting gold layer transformation")
    try:
        conn = connect_to_db()
        transform_to_gold(conn)
    except Exception as e:
        logger.error(f"Error in run_gold: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    run_gold()