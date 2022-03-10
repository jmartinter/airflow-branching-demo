import pendulum
import random

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, get_current_context

DAG_NAME = "04_xcom_with_context_tester"
DEFAULT_ARGS = {
    "owner": "javi",
}


@task
def push_random():
    """Pushes an XCom with a specific target"""
    random_value = random.choice(["foo", "bar", "yay", "baz"])

    context = get_current_context()
    context["ti"].xcom_push(key='random_value', value=random_value)
    print(f"The xcom value pushed is {random_value}")


@task
def pull_random_value():
    context = get_current_context()
    random_value = context["ti"].xcom_pull(key="random_value", task_ids='push_random')
    print(f"The xcom value pulled is {random_value}")


@task
def print_xcom(random_value):
    print(f"The xcom value provided is {random_value}")


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    print_task = PythonOperator(
        task_id='print_xcom',
        python_callable=print_xcom,
        op_args=["{{ ti.xcom_pull(key='random_value', task_ids='push_random') }}"]
    )

    push_random() >> [pull_random_value(), print_task]
