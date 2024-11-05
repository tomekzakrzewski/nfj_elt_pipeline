from google.cloud import storage
from datetime import datetime
import json
import logging

def upload_to_gcs(data: list[dict], bucket_name: str)
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        timestamp = datetime.now().strftime('%Y/%m/%d/%H_%M_%S')
        blob_name = f"raw/job_postings/{timestamp}.json"
        blob = bucket.blob(blob_name)

        blob.upload_from_string(
            json.dumps(data),
            content_type="application/json"
        )

        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        logging.info(f"Successfully uploaded {len(data)} records to {gcs_uri}")

        return gcs_uri

    except Exception as e:
        logging.error(f"Error uploading to GCS: {str(e)}")
        raise
