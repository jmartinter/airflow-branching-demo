import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.utils.db import provide_session
from airflow.models import XCom

from common import DEFAULT_ARGS

DAG_NAME = "07_clean_xcom"
DAGS_NAME_TO_CLEAN = [
    "01_branches",
    "02_xcom_push",
    "03_xcom_return",
    "04_xcom_advanced",
]


@task
@provide_session
def cleanup_xcom(session=None):
    """ Delete all existing stored XCOMs in configured DAGs"""
    session \
        .query(XCom) \
        .filter(XCom.dag_id.in_(DAGS_NAME_TO_CLEAN)) \
        .delete(synchronize_session="fetch")


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    cleanup_xcom()
