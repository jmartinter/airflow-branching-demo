from time import sleep
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


DAG_NAME = "00_trigger_rule_tester"
DEFAULT_ARGS = {
    "owner": "javi",
}


def log_dependencies(ti):
    print(f"All dependencies are met: {ti.are_dependencies_met()}")
    print(f"Failed dependencies: {list(ti.get_failed_dep_statuses())}")


@task
def failed():
    raise AirflowFailException


@task
def all_successed(ti=None):
    sleep(3)
    log_dependencies(ti)


@task(trigger_rule=TriggerRule.ALL_FAILED)
def all_failed(ti=None):
    sleep(3)
    log_dependencies(ti)


@task(trigger_rule=TriggerRule.ONE_SUCCESS)
def one_success(ti=None):
    sleep(3)
    log_dependencies(ti)


@task(trigger_rule=TriggerRule.NONE_FAILED)
def none_failed(ti=None):
    sleep(3)
    log_dependencies(ti)


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def none_failed_one_success(ti=None):
    sleep(3)
    log_dependencies(ti)


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    tags=["demo"],
    catchup=False) as dag:

    fail_task = failed()
    all_successed_task = all_successed()
    all_failed_task = all_failed()
    all_failed_another_task = all_failed()
    one_success_task = one_success()
    none_failed_task = none_failed()
    none_failed_one_success_task = none_failed_one_success()

    fail_task >> all_successed_task
    fail_task >> all_failed_task
    all_failed_task >> all_failed_another_task
    [fail_task, all_successed_task, all_failed_task, all_failed_another_task] >> one_success_task
    [all_failed_task, all_failed_another_task] >> none_failed_task
    [all_failed_task, all_failed_another_task, all_successed_task] >> none_failed_one_success_task
