from google.cloud import storage
from google.api_core import exceptions
from config.config import GCP_CONFIG
import logging

def create_gcs_bucket() -> storage.Bucket:
    bucket_name =GCP_CONFIG["raw_bucket"]
    storage_class = GCP_CONFIG["storage"]
    location = GCP_CONFIG["location"]

    try:
        client = storage.Client()
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
