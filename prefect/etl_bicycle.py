from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.bigquery import BigQueryWarehouse


# pd.read_csv('test.zip',compression='zip')


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