import pendulum
import random

from airflow import DAG
from airflow.decorators import task
from airflow.utils.db import provide_session
from airflow.models import XCom


DAG_NAME = "02_more_xcom_tester"
DEFAULT_ARGS = {
    "owner": "javi",
}


@task
def push_random(ti=None):
    """Pushes an XCom with a specific target"""
    random_value = random.randint(0, 100)
    ti.xcom_push(key='random_value', value=random_value)
    print(f"The xcom value pushed is {random_value}")


@task
def pull_random_value(ti=None):
    random_value = ti.xcom_pull(key="random_value", task_ids='push_random')
    print(f"The xcom value pulled is {random_value}")


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    push_task = push_random()
    pull_one_task = pull_random_value()

    push_task >> pull_one_task
