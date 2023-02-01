from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import json

with open('gcp_credentials.json') as f:
   cred = json.load(f)

credentials_block = GcpCredentials(
    service_account_info=cred
)
credentials_block.save("de-zoomcamp-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("de-zoomcamp-gcp-creds"),
    bucket="de_zoomcamp_taxi_trips"  # insert your own GCS bucket name
)

bucket_block.save("de-zoomcamp-gcs", overwrite=True)