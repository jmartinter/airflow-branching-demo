import pendulum
import random

from airflow import DAG
from airflow.decorators import task
from airflow.utils.session import create_session
from airflow.models import XCom


DAG_NAME = "05_xcom_direct_db_access_tester"
DEFAULT_ARGS = {
    "owner": "javi",
}


@task
def push_random():
    """Pushes an XCom with a specific target"""
    random_value = random.choice(["foo", "bar", "yay", "baz"])

    with create_session() as session:
        xcom = XCom(
            key="random_value",
            value=XCom.serialize_value(random_value),
            timestamp=pendulum.now(tz="UTC"),
            execution_date=pendulum.now(tz="UTC"),  # TODO provide datetime from dag run
            task_id="push_random",
            dag_id=DAG_NAME
        )
        session.add(xcom)

    print(f"The xcom value pushed is {random_value}")


@task
def pull_random_value():
    with create_session() as session:
        random_value = session.query(XCom.value).filter(XCom.task_id == 'push_random', XCom.dag_id == DAG_NAME).one()
        random_value = XCom.deserialize_value(random_value)

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
    pull_task = pull_random_value()

    push_task >> pull_task
