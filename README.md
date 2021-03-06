# airflow-branching-demo
This repo contains the results of some exploratory research on Airflow features focused on learning how to define dynamic task flows.
The question I wanted to repy is basically, how we can define task flows where the execution can be controlled by the results from previous tasks.

## Covered Airflow features
* Trigger rules
* Variables
* Branch Operators
* XComs
* Context
* Session

## DAGs
As a result of the study, a list of operative Airflow DAGs are available:
* `00_trigger_rules_tester_dag.py`
  * Shows how different trigger rules apply to tasks in a pipeline 
* `01_branches_tester_dag.py`
  * Examples with the `BranchPythonOperator`, `ShortCircuitOperator` and `BranchDayOfWeekOperator`
  * Conditions are set on pipeline start time
* `02_xcom_tester_dag.py`
  * The most basic example about writing and reading XComs ever
* `02_xcom_tester_dag_2.py`
  * Some basic notations about how a task can write and read XComs 
* `03_clean_xcom_tester_dag.py`
  * A one-task pipeline to remove all XComs generated by a previous pipeline
  * This task can be included in a pipeline to clean the pipeline state from DB
* `04_xcom_with_context_tester_dag.py`
  * An alternative way to read and write XComs accessing the context
  * Suitable for those operators that are not based on a callable 
* `05_xcom_with_session_tester_dag.py`
  * An alternative way to read and write XComs accessing the context 
  * Suitable for those operators that are not based on a callable 
* `06_final_tester_dag.py`
  * A final wrapper example with a branch operator, which condition is set dynamically by one of the branches 

## How to use
How to install Airflow locally with Docker is detailed in this [Quick Start](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) and the enviroment is initialized, run `docker-compose up`.
In short words, you have to initialize the environment for the first time
```
mkdir -p ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
```
After that just run `docker-compose up` and you are ready to test the DAGS on your [local Airflow](http://localhost:8080)

To clean the environment just:
```
docker-compose down --volumes --remove-orphans command
```