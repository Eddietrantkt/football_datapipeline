# 🏆 Football Data ETL Pipeline

This project is an **ETL data pipeline** built with **Airflow**, **MinIO**, and **PostgreSQL** to extract, transform, and load English Premier League data.

---

## 🧱 Project Architecture

```bash
Football_data/
│
├── assets/
│   ├── bronze.py      # Extract data and upload to MinIO (Bronze Layer)
│   ├── silver.py      # Transform raw data into structured tables (Silver Layer)
│   ├── gold.py        # Aggregate and flatten data for analytics (Gold Layer)
│   ├── init_db.py     # Initialize PostgreSQL schemas and tables
│
├── get_data/
│   ├── fetch_matches.py     # Fetch data from Football API
│   ├── minio_client.py      # Helper functions to interact with MinIO
│   ├── load_from_minio.py   # Read JSON objects from MinIO
│
├── dags/
│   ├── orchestrator.py  # Airflow DAG orchestration
|   |__ gold_pipeline.py # to upload only from silver layer to gold
│
├── docker-compose.yml
├── requirements.txt
└── README.md

Data is from https://www.football-data.org/
<img width="1903" height="902" alt="image" src="https://github.com/user-attachments/assets/38bb8a03-382f-44d9-b8f1-04c5b5c89a2f" />

and we only focus on the Premier league matches
<img width="320" height="320" alt="image" src="https://github.com/user-attachments/assets/cc9a1242-7b8b-4371-892b-914fa18e5d18" />

🚀 How It Works
1. Bronze Layer

Fetches football match data via API and stores raw JSON files into MinIO. 

2. Silver Layer

Cleans and normalizes JSON into relational tables in PostgreSQL.

Tables: dev.silver_PL_team, dev.silver_PL_matches.

3. Gold Layer

Aggregates match data to compute team statistics and league standings.

Tables: dev.gold_team_stats, dev.gold_league_standings.

🧩 Tech Stack

Python 

Apache Airflow

MinIO

PostgreSQL

Docker Compose
🛠️ Usage
Run locally with Docker
docker-compose up -d

Initialize database (only once)
docker exec -it airflow-webserver python assets/init_db.py

Run layers manually
docker exec -it airflow-webserver python assets/bronze.py
docker exec -it airflow-webserver python assets/silver.py
docker exec -it airflow-webserver python assets/gold.py

📊 Visualization

Data from the gold layer can be connected to Apache Superset or Metabase for dashboard visualization.
![leading-board-2025-10-07T08-37-27 827Z](https://github.com/user-attachments/assets/518279e7-b2ef-4a05-a235-302e3bb84f20)

