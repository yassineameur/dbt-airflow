import time
from os import environ
import json

from google.cloud import storage
from google.oauth2 import service_account


def upload_file(file_path: str, gcloud_file_name: str, bucket_name: str):
    token_as_string = environ["SA_KEY_JSON"]
    token = json.loads(token_as_string)

    credentials = service_account.Credentials.from_service_account_info(token)
    client = storage.Client(credentials=credentials, project=token["project_id"])
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcloud_file_name)
    blob.upload_from_filename(file_path)
    print("File uploaded successfully")
    time.sleep(60)


if __name__ == "__main__":
    # TODO find automatically the composer bucket
    upload_file(
        file_path="dbt_dag.py",
        gcloud_file_name="dags/dbt_dag.py",
        bucket_name=environ["COMPOSER_BUCKET"]
    )
