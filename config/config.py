import os

GCP_CONFIG = {
    'raw_bucket': os.getenv("GCP_BUCKET_NAME"),
    'storage': os.getenv("GCP_STORAGE_CLASS"),
    'location': os.getenv("europe-central2")

}

    # GCP_BUCKET_NAME: "job_postings_raw_data"
    # GCP_STORAGE_CLASS: "STANDARD"
    # GCP_LOCATION: "europe-central2"
