from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3) # in case it fails
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, parse_dates=[1, 2], infer_datetime_format=True, low_memory=False) # 2nd and 3rd columns are dates, so we should parse them
    print(f'Data file has {len(df):,} rows')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    print(f'Writing {len(df):,} rows into {dataset_file}.parquet')
    return path

@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load('de-zoomcamp-gcs')
    gcs_block.upload_from_path(
        from_path=f'{path}',
        to_path=path
    )

@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = 'green'
    year = 2020
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    path = write_local(df, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()