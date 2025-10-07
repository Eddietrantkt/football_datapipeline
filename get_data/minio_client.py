from minio import Minio
from minio.error import S3Error
import json
from io import BytesIO
import os
from dotenv import load_dotenv

load_dotenv()

def get_minio_client():
    try:
        client = Minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minio"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minio123"),
            secure=False
        )
        client.bucket_exists("test")  # Test connection
        print("MinIO connection successful")
        return client
    except S3Error as e:
        raise Exception(f"Failed to connect to MinIO: {e}")

def upload_json(client, bucket, key, data):
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Created bucket {bucket}")
        json_bytes = BytesIO(json.dumps(data, indent=4).encode('utf-8'))
        client.put_object(bucket, key, json_bytes, length=len(json_bytes.getvalue()), content_type='application/json')
        print(f"Uploaded {key} to {bucket}")
    except S3Error as e:
        print(f"Error uploading {key} to {bucket}: {e}")
        raise

def download_json(client, bucket, key):
    try:
        response = client.get_object(bucket, key)
        with response:
            data = json.loads(response.data.decode('utf-8'))
            print(f"Downloaded {key} from {bucket}")
            return data
    except S3Error as e:
        print(f"Error downloading {key} from {bucket}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in {key}: {e}")
        return None

def list_objects(client, bucket, prefix=""):
    try:
        if not client.bucket_exists(bucket):
            print(f"Bucket {bucket} does not exist")
            return []
        objects = client.list_objects(bucket, prefix, recursive=True)
        object_names = [obj.object_name for obj in objects]
        print(f"Found {len(object_names)} objects in {bucket}/{prefix}")
        return object_names
    except S3Error as e:
        print(f"Error listing objects in {bucket}/{prefix}: {e}")
        return []