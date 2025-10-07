import requests
import json
import time
import io
from datetime import datetime
from minio.error import S3Error
from get_data.minio_client import get_minio_client, upload_json
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.environ.get("API_KEY", "0eef5feefbc247dab7ccf097bda09c3c")
api_url = "https://api.football-data.org/v4/competitions/PL/matches?season=2025"

HEADERS = {
    "X-Auth-Token": api_key
}
MAX_CALLS_PER_MINUTE = 9   # dưới 10 để an toàn
CALL_INTERVAL = 60 / MAX_CALLS_PER_MINUTE
_last_call_time = 0

def safe_request(url,headers=None):
    global _last_call_time
    now = time.time()
    elapsed = now - _last_call_time
    if elapsed < CALL_INTERVAL:
        sleep_time = CALL_INTERVAL - elapsed
        print(f"Sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)
    response = requests.get(url, headers=headers)
    _last_call_time = time.time()

    if response.status_code == 429:
        print("Rate limit exceeded")
        time.sleep(60)
        return safe_request(url,headers)
def fetch_matches():
    try:
        response = requests.get(api_url, headers=HEADERS, timeout=15)
        if response is None:
            raise ValueError("No response received from API")
        if response.status_code != 200:
            raise ValueError(f"API returned status {response.status_code}: {response.text}")
        print("API request successful")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise



def save_json_to_minio(data, bucket="football", prefix="bronze/matches"):
    """Save raw JSON to MinIO"""
    try:
        client = get_minio_client()
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Created bucket {bucket}")

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        object_name = f"{prefix}/matches_{ts}.json"
        upload_json(client, bucket, object_name, data)
        return f"{bucket}/{object_name}"
    except S3Error as e:
        print(f"Error saving to MinIO: {e}")
        raise

def run_bronze(date_from=None, date_to=None):
    """Run bronze layer: Fetch and save to MinIO"""
    print(f"Starting bronze layer with date_from={date_from}, date_to={date_to}")
    data = fetch_matches()
    path = save_json_to_minio(data)
    print(f"Bronze layer complete. Saved to {path}")
    return path

if __name__ == "__main__":
    run_bronze()
