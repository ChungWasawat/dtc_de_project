from pathlib import Path
import requests, zipfile
from io import BytesIO
import re
import pandas as pd
import numpy as np
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.bigquery import BigQueryWarehouse


## load csv file from web to gbucket
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, file_name: str, year: int, month: int) -> pd.DataFrame:
    print("Downloading zip file started")
    if year == 2022 and (month == 6 or month == 7):
        file_name = f"{year}{month:02}-citbike-tripdata"
        req = requests.get(f"https://s3.amazonaws.com/tripdata/{file_name}.csv.zip")
    else:
        req = requests.get(dataset_url)
    #local_path = f"D:\data\project\{file_name}.csv.zip"
    #with open(local_path,'wb') as output_file:
    #    output_file.write(req.content)
    buffer1 = BytesIO(req.content)
    print("Downloading zip file completed")
    zip_data = zipfile.ZipFile(buffer1, "r")
    #zip_data = zipfile.ZipFile(local_path, "r")
    df = pd.read_csv(zip_data.open(f'{file_name}.csv'))
    zip_data.close()
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
    col_name = df.columns.values.tolist()
    print(col_name)
    # After 2021, data provider changed data format of the data source
    if "starttime" in col_name:
        df.rename(columns={"start station id": "start_station_id", "start station name": "start_station_name", \
                        "start station latitude": "start_station_latitude", "start station longitude": "start_station_longitude", \
                            "end station id": "end_station_id", "end station name": "end_station_name", \
                            "end station latitude": "end_station_latitude", "end station longitude": "end_station_longitude", \
                            "birth year": "birth_year"}, inplace=True)
        
        df['bikeid'] = df['bikeid'].astype(str)

        df['starttime'] = pd.to_datetime(df['starttime'])
        df['stoptime'] = pd.to_datetime(df['stoptime'])

        df['start_station_id'].fillna(0, inplace=True)
        df['end_station_id'].fillna(0, inplace=True)
        df['start_station_name'].fillna("Unknown", inplace=True)
        df['end_station_name'].fillna("Unknown", inplace=True)

        df['bikeid'] = 'A' + df['bikeid'].astype(str)
    
    else:
        df.rename(columns={"ride_id": "bikeid", "started_at": "starttime", "ended_at": "stoptime"}, inplace=True) 

        df['bikeid'] = df['bikeid'].astype(str)

        df['start_station_id'].fillna(0, inplace=True)
        df['end_station_id'].fillna(0, inplace=True)
        df['start_station_name'].fillna("Unknown", inplace=True)
        df['end_station_name'].fillna("Unknown", inplace=True)

        df['starttime'] = pd.to_datetime(df['starttime'])
        df['stoptime'] = pd.to_datetime(df['stoptime'])  
        df['tripduration'] = ( df['stoptime'] - df['starttime'] ) / np.timedelta64(1,'s')
        df['tripduration'] = df['tripduration'].astype(int)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df[["tripduration", "starttime", "stoptime", "start_station_name", "end_station_name", "bikeid" ]]  


def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as transformed parquet file"""
    # execute at data storage folder
    local_path = Path.cwd()
    path = Path(f"data/project/{dataset_file}.parquet")
    path2 = local_path / path

    #for windows
    path3 = Path(f"D:\data\project\{dataset_file}.parquet")

    df.to_parquet(path3, compression="gzip")
    return path3


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
    df['start_station_id'].fillna(0, inplace=True)
    df['start_station_name'].fillna(0, inplace=True)
    df['end_station_id'].fillna(0, inplace=True)
    df['end_station_name'].fillna(0, inplace=True)
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
        df = fetch(dataset_url, dataset_file, year, month)
        df_clean = clean(df)
        path = write_local(df_clean, dataset_file)
        write_gcs(path, dataset_file)
    # gcs to bq
    elif func == 1:
        path = extract_from_gcs(year, month)
        df = transform(path)
        write_bq_table(df)


@flow()
def etl_parent_w2bq_bike_flow(
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
    etl_parent_w2bq_bike_flow(months, year, func)

