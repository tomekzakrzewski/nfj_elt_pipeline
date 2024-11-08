from google.cloud import storage
from google.api_core import exceptions
import logging
from google.cloud.storage import Blob

def create_gcs_bucket(bucket_name: str, storage_class: str, location: str) -> storage.Bucket:
    client = storage.Client()

    try:
        bucket = client.bucket(bucket_name)

        bucket.storage_class = storage_class

        bucket = client.create_bucket(
            bucket,
            location=location
        )
        logging.info(f'Bucket {bucket.name} successfully created.')
        return bucket

    except exceptions.Conflict:
        logging.info(f"Bucket {bucket_name} already exists.")
        return client.get_bucket(bucket_name)

    except Exception as e:
        logging.info(f"Error creating bucket {bucket_name}: {str(e)}")
        raise

def get_latest_blob(bucket_name: str) -> Blob:
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        blobs = list(bucket.list_blobs(prefix='raw/job_postings/'))

        if not blobs:
            raise Exception("No files found in GCS bucket")

        latest_blob: Blob = max(blobs, key=lambda x: x.time_created)
        logging.info(f"Latest file found: {latest_blob.name}, created at: {latest_blob.time_created}")
        return latest_blob
    except Exception as e:
        logging.error(f"Error getting latest file from GCS: {str(e)}")
        raise
