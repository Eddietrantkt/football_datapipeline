import psycopg2
from dotenv import load_dotenv
import logging
import os
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_db():
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

def init_tables():
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""
             CREATE SCHEMA IF NOT EXISTS dev;

            CREATE TABLE IF NOT EXISTS dev.silver_PL_team (
                team_id BIGINT PRIMARY KEY,
                team_name TEXT NOT NULL,
                short_name TEXT,
                tla TEXT,
                crest TEXT
            );

            CREATE TABLE IF NOT EXISTS dev.silver_PL_matches (
                match_id BIGINT PRIMARY KEY,
                competition_id BIGINT,
                season_id BIGINT,
                matchday INT,
                stage TEXT,
                area_id BIGINT,
                area_name TEXT,
                match_date TIMESTAMPTZ NOT NULL,
                last_updated TIMESTAMPTZ,
                match_status TEXT,
                winner TEXT,
                duration TEXT,
                full_time_home INT,
                full_time_away INT,
                half_time_home INT,
                half_time_away INT,
                extra_time_home INT,
                extra_time_away INT,
                penalties_home INT,
                penalties_away INT,
                home_team_id BIGINT REFERENCES dev.silver_PL_team(team_id),
                away_team_id BIGINT REFERENCES dev.silver_PL_team(team_id),

                referee_primary_id BIGINT,
                referee_primary_name TEXT,
                referee_primary_nationality TEXT,
                referees JSONB,

                raw_json JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS dev.gold_team_stats (
                team_id BIGINT PRIMARY KEY,
                team_name TEXT NOT NULL,
                matches_played INT,
                wins INT,
                losses INT,
                draws INT,
                total_goals_scored INT,
                total_goals_conceded INT,
                goal_difference INT,
                avg_goals_scored FLOAT,
                avg_goals_conceded FLOAT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS dev.gold_league_standings (
                competition_id BIGINT,
                team_id BIGINT,
                team_name TEXT NOT NULL,
                rank INT,
                points INT,
                goal_difference INT,
                matches_played INT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (competition_id, team_id)
            );

            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_silver_PL_matches_date ON dev.silver_PL_matches(match_date);
            CREATE INDEX IF NOT EXISTS idx_silver_PL_matches_home_team ON dev.silver_PL_matches(home_team_id);
            CREATE INDEX IF NOT EXISTS idx_silver_PL_matches_away_team ON dev.silver_PL_matches(away_team_id);
            CREATE INDEX IF NOT EXISTS idx_gold_team_stats_team_id ON dev.gold_team_stats(team_id);
            CREATE INDEX IF NOT EXISTS idx_gold_league_standings_competition_id ON dev.gold_league_standings(competition_id);
        """)
        conn.commit()
        cur.close()
        logger.info("Database schema and tables initialized")
    except psycopg2.Error as e:
        logger.error(f"Error initializing tables: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    init_tables()