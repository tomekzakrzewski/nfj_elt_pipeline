import os

GCP_CONFIG = {
    'raw_bucket': str(os.getenv("GCP_BUCKET_NAME")),
    'storage': str(os.getenv("GCP_STORAGE_CLASS")),
    'location': str(os.getenv("GCP_LOCATION"))
}

    # GCP_BUCKET_NAME: "job_postings_raw_data"
    # GCP_STORAGE_CLASS: "STANDARD"
    # GCP_LOCATION: "europe-central2"
