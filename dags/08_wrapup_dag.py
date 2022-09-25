import random
import pendulum

from textwrap import dedent

from airflow import DAG
from airflow.models.variable import Variable
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from common import DEFAULT_ARGS


DAG_NAME = "08_wrapup"


@task
def detect_language(ti=None):
    language = random.choice(["EN", "ES", "FR"])

    ti.xcom_push(key="INPUT_LANGUAGE", value=language)


def lang_selector_func(ti=None):

    detected_language = ti.xcom_pull(key="INPUT_LANGUAGE", task_ids='detect_language')

    return [f"process_{detected_language}"]


def run_evaluation_func(annotations_file):
    if annotations_file != "":
        return ["load_annotations"]
    return []


def evaluate():
    simulate_failure = Variable.get("wrapup__evaluate_task_fails", default_var=False, deserialize_json=True)
    if simulate_failure:
        raise Exception("Ups! An error happened")


with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
) as dag:

    dag.doc_md = dedent(
        """\
    #### Test for different branches options
    ```
    {
        "annotations_file": "foo"
    }
    ``` 
    """

    # Regular branch
    )
    load_input = DummyOperator(task_id='load_input')

    select_language = BranchPythonOperator(
        task_id="select_language",
        python_callable=lang_selector_func,
    )

    process_en = DummyOperator(task_id='process_EN')
    process_es = DummyOperator(task_id='process_ES')
    process_fr = DummyOperator(task_id='process_FR')

    save_results = DummyOperator(task_id='save_results', trigger_rule=TriggerRule.ONE_SUCCESS)

    send_notification = DummyOperator(task_id='send_notification', trigger_rule=TriggerRule.NONE_FAILED)
    send_error = DummyOperator(task_id='send_error', trigger_rule=TriggerRule.ONE_FAILED)

    load_input >> detect_language() >> select_language >> [process_en, process_es, process_fr] >> save_results

    # Evaluation branch
    run_evaluation = BranchPythonOperator(
        task_id='run_evaluation',
        python_callable=run_evaluation_func,
        op_args=[
            "{{ dag_run.conf.get('annotations_file', '') }}",
        ],
    )

    load_annotations = DummyOperator(task_id='load_annotations')

    evaluate_results = PythonOperator(
        task_id='evalute_results',
        python_callable=evaluate
    )

    run_evaluation >> load_annotations

    [load_annotations, save_results] >> evaluate_results

    [save_results, evaluate_results] >> send_notification

    [save_results, evaluate_results] >> send_error

    # Note: this doesn't work if ShortCircuitOperator is used to start the evaluation branch
    # This opertator skips any downstream task, even the ones depending on other streams
