import pendulum
import random

from airflow import DAG
from airflow.decorators import task
from airflow.utils.session import create_session
from airflow.utils.db import provide_session
from airflow.models import XCom

from common import DEFAULT_ARGS

DAG_NAME = "06_xcom_with_session"

XCOM_KEY = "RANDOM_VALUE"


@task
def push_random(ts=None):
    """Pushes an XCom with a specific target"""
    random_value = random.choice(["FOO", "BAR", "YAY", "BAZ"])

    with create_session() as session:
        xcom = XCom(
            key=XCOM_KEY,
            value=XCom.serialize_value(random_value),
            timestamp=pendulum.now(tz="UTC"),
            execution_date=pendulum.parse(ts),
            task_id="push_random",
            dag_id=DAG_NAME
        )
        session.add(xcom)

    print(f"The xcom value pushed is {random_value}")


@task
def pull_random_value(ts=None):
    with create_session() as session:
        random_value = session \
            .query(XCom.value) \
            .filter(
                XCom.key == XCOM_KEY,
                XCom.task_id == 'push_random',
                XCom.dag_id == DAG_NAME,
                XCom.execution_date == pendulum.parse(ts)
            ) \
            .one()
        random_value = XCom.deserialize_value(random_value)

    print(f"The xcom value pulled is {random_value}")


@task
@provide_session
def pull_all_values(session=None):
    """ Return all historic xcoms from a given task"""
    xcoms = session \
        .query(XCom.key, XCom.value, XCom.execution_date) \
        .filter(XCom.task_id == "push_random", XCom.dag_id == DAG_NAME).all()

    for xcom in xcoms:
        print(f"XCom key \"{xcom.key}\" with value {xcom.value.decode()} was pushed by run on {xcom.execution_date}")


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
    pull_all_task = pull_all_values()

    push_task >> [pull_task, pull_all_task]
