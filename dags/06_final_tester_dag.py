import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


DAG_NAME = "final_tester"
DEFAULT_ARGS = {
    "owner": "javi",
}


@task
def initial_processing():
    context = get_current_context()
    context["ti"].xcom_push(key="requires_extra_step", value=False)


def selector_func(ti=None):
    tasks = ["standard_processing"]

    extra = ti.xcom_pull(key="requires_extra_step", task_ids='initial_processing')
    if extra:
        tasks.append('extra_processing')
    return tasks


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    selector = BranchPythonOperator(
        task_id="variable_branch",
        python_callable=selector_func,
    )

    standard_processing = DummyOperator(task_id='standard_processing')
    extra_processing = DummyOperator(task_id='extra_processing')
    send_notification = DummyOperator(task_id='send_notification', trigger_rule=TriggerRule.ALWAYS)

    initial_processing() >> selector >> [standard_processing, extra_processing] >> send_notification
