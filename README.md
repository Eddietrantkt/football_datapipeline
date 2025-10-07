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
