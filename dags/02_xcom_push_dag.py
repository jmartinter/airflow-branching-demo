import pendulum

from airflow import DAG
from airflow.decorators import task

from common import DEFAULT_ARGS

DAG_NAME = "02_xcom_push"

XCOM_KEY = "MY_FIRST_XCOM"
XCOM_VALUE = "FOO"


@task
def push_xcom(ti=None):
    """Pushes an XCom with a specific target"""
    ti.xcom_push(key=XCOM_KEY, value=XCOM_VALUE)
    print(f"The xcom value pushed is {XCOM_VALUE}")


@task
def pull_xcom(ti=None):
    xcom_value = ti.xcom_pull(key=XCOM_KEY, task_ids='push_xcom')
    print(f"The xcom value pulled is {xcom_value}")

    assert xcom_value == XCOM_VALUE


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    push_xcom() >> pull_xcom()
