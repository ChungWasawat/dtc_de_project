from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.bigquery import BigQueryWarehouse

## load parquet file from nyc web to gbucket
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_parquet(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, colour: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if colour == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif colour == "green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype('Int64')
    elif colour == "fhv":
        """Rename columns"""
        df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
        df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
        df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])


    
    if colour == "yellow" or colour == "green":
        df["VendorID"] = df["VendorID"].astype('Int64')
        df["RatecodeID"] = df["RatecodeID"].astype('Int64')
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')
        df["passenger_count"] = df["passenger_count"].astype('Int64')
        df["payment_type"] = df["payment_type"].astype('Int64')
        
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df    


def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as transformed parquet file"""
    # execute at data storage folder
    local_path = Path.cwd()
    path = Path(f"data/{color}/{dataset_file}.parquet")
    path2 = local_path / path

    #for windows
    path3 = Path(f"D:\data\{color}\{dataset_file}.parquet")

    df.to_parquet(path3, compression="gzip")
    return path2


@task()
def write_gcs(path: Path, colour: str, dataset_file: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtcde-prefect-gcs")
    gBucket_path = f"data/{colour}/{dataset_file}.parquet"
    gcs_block.upload_from_path(from_path=path, to_path=gBucket_path)
    return


## load data from gcs to bq
@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtcde-prefect-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path, colour: str) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    if colour != "fhv":
        print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
        df["passenger_count"].fillna(0, inplace=True)
        print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    else:
        print(f"pre: null SR flag count: {df['SR_Flag'].isna().sum()}")
        df["SR_Flag"].fillna(0, inplace=True)
        print(f"post: null SR flag count: {df['SR_Flag'].isna().sum()}")
    return df


@task()
def write_bq_table(df: pd.DataFrame, colour: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtcde-prefect-gcp-creds")
    if colour == "yellow":
        dest_table = "project_all_dataset.taxi_yellow"
    elif colour == "green":
        dest_table = "project_all_dataset.taxi_green"
    elif colour == "fhv":
        dest_table = "project_all_dataset.taxi_fhv"

    df.to_gbq(
        destination_table=dest_table,
        project_id="mp-dtc-data-eng",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


## flow
@flow()
def etl_web_to_bq(year: int, month: int, color: str, func: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    
    # web to gcs
    if func == 0:
        df = fetch(dataset_url)
        df_clean = clean(df, color)
        path = write_local(df_clean, color, dataset_file)
        write_gcs(path, color, dataset_file)
    # gcs to bq
    elif func == 1:
        path = extract_from_gcs(color, year, month)
        df = transform(path, color)
        write_bq_table(df, color)


@flow()
def etl_parent_w2bq_taxi_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow", func: int = 0
    ):
    for month in months:
        etl_web_to_bq(year, month, color, func)


if __name__ == "__main__":
    color = "yellow"
    # months = [1]
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019 # 2019 & 2022
    # func = 0(web to gcs) / 1(gcs to bq)
    func = 0
    etl_parent_w2bq_flow(months, year, color, func)