import pendulum

from airflow import DAG
from airflow.decorators import task

from common import DEFAULT_ARGS

DAG_NAME = "04_xcom_advanced"

XCOM_KEY_1 = "XCOM_1"
XCOM_VALUE_1 = "THIS IS ONE XCOM"

XCOM_KEY_2 = "XCOM_2"
XCOM_VALUE_2 = "AND THIS IS ANOTHER XCOM"


@task
def return_multiple_xcoms():
    """Pushes an XCom with a specific target"""
    return XCOM_VALUE_1, XCOM_VALUE_2


@task
def return_multiple_xcoms_as_dict():
    """Pushes an XCom with a specific target"""
    return {
        XCOM_KEY_1: XCOM_VALUE_1,
        XCOM_KEY_2: XCOM_VALUE_2
    }


@task
def pull_multiple_xcoms_from_return(ti=None):
    """Pushes an XCom without a default target"""
    xcom_values = ti.xcom_pull(key="return_value", task_ids='return_multiple_xcoms')
    print(f"The xcom values pulled from is {xcom_values}")

    assert xcom_values == [XCOM_VALUE_1, XCOM_VALUE_2]

    xcom_values_as_dict = ti.xcom_pull(key="return_value", task_ids='return_multiple_xcoms_as_dict')
    print(f"The xcom value dictionary pulled is {xcom_values_as_dict}")

    assert xcom_values_as_dict == {XCOM_KEY_1: XCOM_VALUE_1, XCOM_KEY_2: XCOM_VALUE_2}


@task
def pull_values_from_another_dag(ti=None):
    """ Recover a XCOM written by a different DAG"""

    XCOM_FROM_02 = "MY_FIRST_XCOM"
    PREVIOUS_DAG = "02_xcom_push"

    xcom_value = ti.xcom_pull(
        key=XCOM_FROM_02, dag_id=PREVIOUS_DAG, task_ids='push_xcom', include_prior_dates=True
    )
    print(f"Remember by first xcom value pulled was {xcom_value}")

    assert xcom_value == "FOO"


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    pull_values_from_another_dag()

    # Return xcoms
    [return_multiple_xcoms(), return_multiple_xcoms_as_dict()] >> pull_multiple_xcoms_from_return()
