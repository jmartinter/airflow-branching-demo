import pendulum

from textwrap import dedent

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.edgemodifier import Label

from common import DEFAULT_ARGS

DAG_NAME = "00_branches"


def input_launcher(both_paths: str):
    paths = ["first"]
    if both_paths == "yes":
        paths.append("second")
    return paths


def variable_launcher():
    enable = Variable.get("branches__enable_extra_task", default_var=False, deserialize_json=True)
    paths = ["third"]
    if enable:
        paths.append("extra")
    return paths


def shortcircuit_launcher():
    run = Variable.get("branches__run_fourth", default_var=False, deserialize_json=True)
    return run


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
        "both": "yes" / "no" (default "no"),
    }
    ``` 
    """
    )

    input_branch = BranchPythonOperator(
        task_id="input_branch",
        python_callable=input_launcher,
        op_args=[
            "{{ dag_run.conf.get('both', '') }}",
        ],
    )

    variable_branch = BranchPythonOperator(
        task_id="variable_branch",
        python_callable=variable_launcher,
    )

    shorcircuit_branch = ShortCircuitOperator(
        task_id='shortcircuit',
        python_callable=shortcircuit_launcher,
    )

    weekday_branch = BranchDayOfWeekOperator(
        task_id="weekday_branch",
        follow_task_ids_if_true="thursday",
        follow_task_ids_if_false="other_day",
        week_day="Thursday",
    )

    first = DummyOperator(task_id='first')
    second = DummyOperator(task_id='second')
    third = DummyOperator(task_id='third')
    fourth = DummyOperator(task_id='fourth')
    extra = DummyOperator(task_id='extra')
    thursday = DummyOperator(task_id='thursday')
    other_day = DummyOperator(task_id='other_day')

    input_branch >> Label("always") >> first
    input_branch >> Label("if input param") >> second

    first >> variable_branch
    variable_branch >> Label("always") >> third
    variable_branch >> Label("if variable enabled") >> extra

    third >> shorcircuit_branch >> Label("if variable enabled") >> fourth

    third >> weekday_branch
    weekday_branch >> Label("if today Thursday") >> thursday
    weekday_branch >> Label("if today not Thursday") >> other_day
