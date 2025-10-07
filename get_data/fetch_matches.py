import requests
import io
import json
from datetime import datetime
from minio.error import S3Error
from minio_client import get_minio_client, upload_json
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.environ.get("API_KEY", "0eef5feefbc247dab7ccf097bda09c3c")
api_url = "https://api.football-data.org/v4/matches"

HEADERS = {
    "X-Auth-Token": api_key
}

def fetch_matches():
    try:
        response = requests.get(api_url, headers=HEADERS)
        response.raise_for_status()
        print("API request successful")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise

def save_json_to_minio(data, bucket="football", prefix="bronze/matches"):
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

if __name__ == "__main__":
    data = fetch_matches()
    path = save_json_to_minio(data)
    print("Saved to", path)