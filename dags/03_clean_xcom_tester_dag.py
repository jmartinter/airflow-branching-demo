import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.utils.db import provide_session
from airflow.models import XCom


DAG_NAME = "03_clean_xcom"
DAG_NAME_TO_REMOVE = "02_xcom_tester_dag_2"  # Provide the desired dag name to remove xcoms

DEFAULT_ARGS = {
    "owner": "javi",
}


@task
@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME_TO_REMOVE).delete()


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    cleanup_xcom()
