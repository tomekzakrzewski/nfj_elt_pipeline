from google.cloud import storage
from google.api_core import exceptions

def create_gcs_bucket(bucket_name, storage_class='STANDARD', location='europe-central2'):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    try:
        bucket.storage_class = storage_class
        bucket = client.create_bucket(bucket, location=location)
        print(f'Bucket {bucket.name} successfully created.')
        return bucket
    except exceptions.Conflict:
        print(f"Bucket {bucket_name} already exists.")
        return client.get_bucket(bucket_name)
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {str(e)}")
        raise
