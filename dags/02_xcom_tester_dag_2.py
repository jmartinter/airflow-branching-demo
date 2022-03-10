import pendulum
import random

from airflow import DAG
from airflow.decorators import task
from airflow.utils.db import provide_session
from airflow.models import XCom


DAG_NAME = "02_xcom_tester_dag_2"
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
def return_random():
    """Pushes an XCom without a default target"""
    random_value = random.randint(100, 200)
    other_random_value = random.randint(100, 200)
    print(f"The xcom value pushed is {random_value}")
    return {"one": random_value, "other": other_random_value}


@task(multiple_outputs=True)
def return_multiple_random():
    """Pushes an XCom without a default target"""
    random_value = random.randint(100, 200)
    other_random_value = random.randint(100, 200)
    print(f"The xcom value pushed is {random_value}")
    return {"one": random_value, "other": other_random_value}


@task
def pull_random_value(ti=None):
    random_value = ti.xcom_pull(key="random_value", task_ids='push_random')
    print(f"The xcom value pulled is {random_value}")


@task
def pull_all_values(ti=None):
    # TODO I wasn't able to make this run
    random_values = ti.xcom_pull(key="random_value", task_ids='push_random', include_prior_dates=True)
    print(f"The xcom values pulled are {random_values}")


@task
@provide_session
def pull_all_values_from_db(session=None):
    random_values = session \
        .query(XCom.value, XCom.execution_date) \
        .filter(XCom.task_id == 'push_random', XCom.dag_id == DAG_NAME).all()
    print(f"The xcom values pulled by session are {random_values}")


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    push_task = push_random()
    return_task = return_random()
    return_multiple_task = return_multiple_random()
    pull_one_task = pull_random_value()
    pull_all_task = pull_all_values()
    pull_all_db_task = pull_all_values_from_db()

    push_task >> return_task >> return_multiple_task >> pull_one_task >> pull_all_task >> pull_all_db_task
