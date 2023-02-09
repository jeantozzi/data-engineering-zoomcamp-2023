from pathlib import Path
import wget
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3) # in case it fails
def fetch(dataset_url: str, file_name: str) -> Path:
    """Download taxi data from GitHub repository"""
    file_path = wget.download(dataset_url, out=f'data/fhv/{file_name}')
    return Path(file_path)

@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local data file to GCS"""
    gcs_block = GcsBucket.load('de-zoomcamp-gcs')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )

@flow(log_prints=True)
def github_to_gcs(year: int, months: list[int]) -> None:
    """The main download-upload function"""
    for month in months:
        file_name = f'fhv_tripdata_{year}-{month:02}.csv.gz'
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file_name}'
        path = fetch(dataset_url, file_name)
        write_gcs(path)

if __name__ == '__main__':
    github_to_gcs()