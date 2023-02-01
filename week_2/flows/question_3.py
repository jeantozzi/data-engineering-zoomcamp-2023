from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3) # in case it fails
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, parse_dates=[1, 2], infer_datetime_format=True) # 2nd and 3rd columns are dates, so we should parse them
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
        from_path=path,
        to_path=path
    )

@flow(log_prints=True)
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    path = write_local(df, color, dataset_file)
    write_gcs(path)
    del(df) # to free memory

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GSC"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('de-zoomcamp-gcs')
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path="./"
    )
    return Path(gcs_path)

@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load('de-zoomcamp-gcp-creds')
    df.to_gbq(
        destination_table='dezoomcamp.yellow_rides',
        project_id='dtc-de-zoomcamp-jt',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )
    print(f'Writing {len(df):,} rows into BigQuery')
    del(df) # to free memory

@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    """"Main ETL flow to load data into BigQuery"""
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    number_of_rows = len(df)
    del(df) # to free memory
    return number_of_rows

@flow(log_prints=True)
def etl_parent_flow(color: str, year: int, months: list[int]) -> None:
    total_rows_to_bq = 0
    for month in months:
        etl_web_to_gcs(color, year, month)
        total_rows_to_bq += etl_gcs_to_bq(color, year, month)
    print(f'Number of rows inserted into BigQuery: {total_rows_to_bq:,}')

if __name__ == '__main__':
    etl_parent_flow()