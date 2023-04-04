from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.bigquery import BigQueryWarehouse


"""
df['starttime'] = pd.to_datetime(df['starttime'])
df['stoptime'] = pd.to_datetime(df['stoptime'])


def changeGender(num:int) -> str:
    if num == 0:
        return "unknown"
    elif num == 1:
        return "male"
    elif num == 2:
        return "female"

df['gender'] = df['gender'].apply(changeGender)
"""

## load csv file from web to gbucket
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_url, compression='zip')
    return df


#dbt
def changeGender(num:int) -> str:
    if num == 0:
        return "unknown"
    elif num == 1:
        return "male"
    elif num == 2:
        return "female"


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df['starttime'] = pd.to_datetime(df['starttime'])
    df['stoptime'] = pd.to_datetime(df['stoptime'])

    df.rename(columns={"start station id": "start_station_id", "start station name": "start_station_name", \
                       "start station latitude": "start_station_latitude", "start station longitude": "start_station_longitude", \
                        "end station id": "end_station_id", "end station name": "end_station_name", \
                        "end station latitude": "end_station_latitude", "end station longitude": "end_station_longitude", \
                        "birth year": "birth_year"})

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df  


def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as transformed parquet file"""
    # execute at data storage folder
    local_path = Path.cwd()
    path = Path(f"data/project/{dataset_file}.parquet")
    path2 = local_path / path

    df.to_parquet(path2, compression="gzip")
    return path2


@task()
def write_gcs(path: Path, dataset_file: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtcde-prefect-gcs")
    gBucket_path = f"data/project/{dataset_file}.parquet"
    gcs_block.upload_from_path(from_path=path, to_path=gBucket_path)
    return


## load data from gcs to bq
@task(retries=3)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/project/{year}{month:02}-citibike-tripdata.parquet"
    gcs_block = GcsBucket.load("dtcde-prefect-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    print(f"pre: missing trip distance: {df.isnull().sum().sum()}")
    df['start station id'].fiillna(0, inplace=True)
    df['start station name'].fiillna(0, inplace=True)
    df['stop station id'].fiillna(0, inplace=True)
    df['stop station name'].fiillna(0, inplace=True)
    print(f'post: missing trip distance: {df.isnull().sum().sum()}')

    return df


@task()
def write_bq_table(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtcde-prefect-gcp-creds")
    dest_table = "project_all_dataset.bike"

    df.to_gbq(
        destination_table=dest_table,
        project_id="mp-dtc-data-eng",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


## flow
@flow()
def etl_web_to_bq(year: int, month: int, func: int) -> None:
    """The main ETL function"""
    dataset_file = f"{year}{month:02}-citibike-tripdata"
    dataset_url = f"https://s3.amazonaws.com/tripdata/{dataset_file}.csv.zip"

    # web to gcs
    if func == 0:
        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, dataset_file)
        write_gcs(path, dataset_file)
    # gcs to bq
    elif func == 1:
        path = extract_from_gcs(year, month)
        df = transform(path)
        write_bq_table(df)


@flow()
def etl_parent_w2bq_flow(
    months: list[int] = [1, 2], year: int = 2021, func: int = 0
    ):
    for month in months:
        etl_web_to_bq(year, month, func)


if __name__ == "__main__":
    # months = [1]
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019 # 2019 & 2022
    # func = 0(web to gcs) / 1(gcs to bq)
    func = 0
    etl_parent_w2bq_flow(months, year, func)

