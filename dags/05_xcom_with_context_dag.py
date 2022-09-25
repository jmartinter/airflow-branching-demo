import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, get_current_context

from common import DEFAULT_ARGS


DAG_NAME = "05_xcom_with_context"

XCOM_KEY = "ANOTHER_XCOM_MORE"
XCOM_VALUE = "YAY"


@task
def push_xcom():
    """Pushes an XCom with a specific target"""
    context = get_current_context()
    context["ti"].xcom_push(key=XCOM_KEY, value=XCOM_VALUE)
    print(f"The xcom value pushed is {XCOM_VALUE}")


@task
def pull_xcom():
    context = get_current_context()
    value = context["ti"].xcom_pull(key=XCOM_KEY, task_ids='push_xcom')
    print(f"The xcom value pulled is {value}")

    assert value == XCOM_VALUE


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    push_xcom() >> pull_xcom()
