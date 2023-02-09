from pathlib import Path
import wget
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3) # in case it fails
def fetch(dataset_url: str) -> Path:
    """Read taxi data from GitHub repository into pandas DataFrame"""
    df = pd.read_csv(dataset_url, parse_dates=[1, 2], infer_datetime_format=True) # 2nd and 3rd columns are dates, so we should parse them
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f'data/fhv/{dataset_file}.parquet.gz')
    df.to_parquet(path, compression='gzip')
    del(df) # to free memory
    return path

@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local data file to GCS"""
    gcs_block = GcsBucket.load('de-zoomcamp-gcs')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )

@flow(log_prints=True)
def github_parquet_to_gcs(year: int, months: list[int]) -> None:
    """The main download-upload function"""
    for month in months:
        file_name = f'fhv_tripdata_{year}-{month:02}'
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file_name}.csv.gz'
        df = fetch(dataset_url)
        path = write_local(df, file_name)
        write_gcs(path)
        del(df) # to free memory

if __name__ == '__main__':
    github_parquet_to_gcs()