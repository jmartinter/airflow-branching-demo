import pendulum

from airflow import DAG
from airflow.decorators import task

from common import DEFAULT_ARGS


DAG_NAME = "03_xcom_return"

XCOM_KEY = "return_value"
XCOM_VALUE = "BAR"


@task
def return_xcom():
    """Pushes an XCom with a specific target"""
    print(f"The xcom value pushed is {XCOM_VALUE}")
    return XCOM_VALUE


@task
def pull_xcom(ti=None):
    """Reads an XCom with a specific target"""
    xcom_value = ti.xcom_pull(key="return_value", task_ids='return_xcom')
    print(f"The xcom value pulled is {xcom_value}")

    assert xcom_value == XCOM_VALUE


@task
def read_xcom(value):
    print(f"The xcom value read is {value}")

    assert value == XCOM_VALUE


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    my_value = return_xcom()

    my_value >> pull_xcom()

    read_xcom(my_value)
