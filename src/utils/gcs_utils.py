from google.cloud import storage
from google.api_core import exceptions
import logging

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
