import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from operators.extract_posts import api_connect

import pandas as pd
from datetime import datetime
from pathlib import Path

import praw

now = datetime.now()
CURRENT_TIME = now.strftime("%H.%M")
RAW_DATA_PATH = (Path(__file__).parent / '../data/raw/').resolve()

CLIENT_ID = Variable.get("CLIENT_ID")
SECRET_KEY = Variable.get("SECRET_KEY")
USER_NAME = Variable.get("USER_NAME")
USER_PASSWOD = Variable.get("USER_PASSWORD")
USER_AGENT = Variable.get("USER_AGENT")

SUBREDDIT = 'belarus'
TIME_FILTER = "day"
LIMIT = 5

POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",  # time when the message was created / Unix Time
    "url",
    "upvote_ratio",
    "spoiler",
)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="get_connection_dag",
    schedule_interval="@daily",
    default_args=default_args,
    # catchup=False,
    max_active_runs=1,
    tags=['de-newssubreddit'],
) as dag:

    get_api_connection = PythonOperator(
        task_id="get_connection_task",
        python_callable=api_connect,
    )

    get_api_connection
    