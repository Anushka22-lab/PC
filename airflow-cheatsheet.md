# Apache Airflow Cheatsheet

Written for **Airflow 3.x** (uses `airflow.sdk`, `airflow api-server`, `airflow dag-processor`). Airflow 2.x differences are noted.

## 1. Core concepts

| Term | Meaning |
|---|---|
| DAG | Directed Acyclic Graph: a workflow made of tasks and dependencies |
| Task | One unit of work in a DAG |
| Operator | Template for a task (`PythonOperator`, `BashOperator`, ...) |
| Task instance | A task in one specific DAG run |
| DAG run | One execution of a DAG for a logical date |
| Scheduler | Decides what to run and when |
| DAG processor | Parses the DAG files (a separate process in Airflow 3) |
| API server | Serves the UI and API (Airflow 3, replaces `webserver`) |
| Executor | How tasks actually run (`LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`) |
| XCom | Small data passed between tasks |
| Connection / Variable | Stored credentials / config values |

## 2. Install and environment

```bash
python3 -m venv .venv
source .venv/bin/activate                # Ubuntu / Linux / macOS
# Windows: .venv\Scripts\activate

# recommended: install with the official constraints file
AIRFLOW_VERSION=3.0.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

airflow version
export AIRFLOW_HOME=~/airflow            # default location of config, dags, logs, db
```

## 3. Running Airflow (what worked in practical)

All in separate terminals with the venv activated:

```bash
airflow db migrate                       # create/upgrade metadata DB (SQLite by default)
airflow dag-processor                    # parses DAG files  (Airflow 3)
airflow scheduler                        # schedules and runs tasks
airflow api-server --port 8080           # UI at http://localhost:8080
```

One-command dev mode (starts everything, prints an admin password):
```bash
airflow standalone
```

Airflow 2.x: `airflow webserver --port 8080` instead of `api-server`, and no separate dag-processor.

Where are DAGs?
```bash
airflow config get-value core dags_folder     # e.g. /home/user/airflow/dags
ls -la ~/airflow/dags
cp aiops_dag.py ~/airflow/dags/
```

Then:
```bash
airflow dags list | grep aiops
airflow dags list-import-errors               # shows files that failed to parse
```

If the DAG doesn't show: DAG processor not running, syntax/import error, or file is not in `dags_folder`.

## 4. Minimal DAG (Airflow 3 style, from `aiops_dag.py`)

```python
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

def collect_metrics():
    return {"cpu": 87, "memory": 65}          # return value is stored as XCom "return_value"

def detect_anomaly(**kwargs):
    metrics = kwargs["ti"].xcom_pull(task_ids="collect_metrics")
    print("Anomaly" if metrics["cpu"] > 80 else "OK")

with DAG(
    dag_id="aiops_workflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,                             # manual trigger only
    catchup=False,
) as dag:
    collect = PythonOperator(task_id="collect_metrics", python_callable=collect_metrics)
    detect  = PythonOperator(task_id="detect_anomaly",  python_callable=detect_anomaly)

    collect >> detect
```

### Import differences

| | Airflow 2.x | Airflow 3.x |
|---|---|---|
| DAG | `from airflow import DAG` | `from airflow.sdk import DAG` |
| PythonOperator | `airflow.operators.python` | `airflow.providers.standard.operators.python` |
| BashOperator | `airflow.operators.bash` | `airflow.providers.standard.operators.bash` |
| Decorators | `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` |

The older imports (`airflow.operators.python`) still appear in `practical2.py` and `practice_dag.py`; they work in 2.x and with deprecation warnings in 3.x if the standard provider is installed.

## 5. TaskFlow API (decorators, less boilerplate)

```python
from datetime import datetime
from airflow.sdk import dag, task

@dag(dag_id="taskflow_demo", start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def pipeline():
    @task
    def extract():
        return {"cpu": 90}

    @task
    def transform(data: dict):
        return data["cpu"] > 80

    @task
    def load(is_anomaly: bool):
        print("anomaly:", is_anomaly)

    load(transform(extract()))                 # dependencies + XComs inferred

pipeline()
```

## 6. Dependencies

```python
a >> b >> c                 # a, then b, then c
a >> [b, c] >> d            # fan-out then fan-in
[a, b] >> c
a.set_downstream(b)         # same as a >> b
b.set_upstream(a)           # same as a >> b

from airflow.sdk import chain
chain(a, [b, c], d)
```

## 7. Scheduling

```python
schedule=None                       # manual only
schedule="@daily"                   # presets: @once @hourly @daily @weekly @monthly @yearly
schedule="0 6 * * *"                # cron: 06:00 every day
schedule="*/15 * * * *"             # every 15 minutes
from datetime import timedelta
schedule=timedelta(hours=2)         # interval
catchup=False                       # do not backfill missed intervals
max_active_runs=1
```

Cron: `min hour day-of-month month day-of-week`.

## 8. Common operators

```python
from airflow.providers.standard.operators.bash import BashOperator
BashOperator(task_id="hello", bash_command="echo 'hi' && date")

from airflow.providers.standard.operators.empty import EmptyOperator
start = EmptyOperator(task_id="start")

from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
PythonOperator(task_id="t", python_callable=fn, op_kwargs={"x": 1}, op_args=[2])

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
```

Branching:
```python
def choose(**kwargs):
    return "alert" if kwargs["ti"].xcom_pull(task_ids="detect") else "skip"
branch = BranchPythonOperator(task_id="branch", python_callable=choose)
branch >> [alert, skip]
```

## 9. XComs

```python
# push (explicit)
kwargs["ti"].xcom_push(key="cpu", value=87)
# pull
kwargs["ti"].xcom_pull(task_ids="collect_metrics", key="cpu")        # explicit key
kwargs["ti"].xcom_pull(task_ids="collect_metrics")                   # return_value
```
Keep XComs small (they live in the metadata DB). Pass paths or IDs, not big data.

## 10. Task settings

```python
PythonOperator(
    task_id="t",
    python_callable=fn,
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=30),
    trigger_rule="all_success",         # all_success | all_failed | one_failed | one_success | none_failed | all_done | always
    pool="default_pool",
    depends_on_past=False,
)
```
Set defaults for all tasks: `DAG(..., default_args={"retries": 2, "retry_delay": timedelta(minutes=1)})`.

## 11. Templating (Jinja) and context

```python
BashOperator(task_id="d", bash_command="echo {{ ds }} {{ logical_date }} {{ dag_run.run_id }}")
```
Common: `{{ ds }}` (YYYY-MM-DD), `{{ ds_nodash }}`, `{{ logical_date }}`, `{{ params.x }}`, `{{ var.value.my_var }}`, `{{ conn.my_conn.host }}`.

In Python callables: `def fn(**context): context["ti"], context["dag_run"], context["params"]`.

## 12. CLI reference

### DAGs
```bash
airflow dags list
airflow dags list-import-errors
airflow dags show aiops_workflow                 # graph
airflow dags details aiops_workflow
airflow dags trigger aiops_workflow
airflow dags trigger aiops_workflow --conf '{"cpu": 95}'
airflow dags pause aiops_workflow
airflow dags unpause aiops_workflow
airflow dags delete aiops_workflow
airflow dags list-runs aiops_workflow
airflow dags next-execution aiops_workflow
airflow dags reserialize                         # force re-parse
airflow dags test aiops_workflow                 # run whole DAG locally, no scheduler needed
airflow dags backfill aiops_workflow --start-date 2026-09-01 --end-date 2026-09-05
```

### Tasks
```bash
airflow tasks list aiops_workflow
airflow tasks test aiops_workflow collect_metrics 2026-09-20     # run one task, no DB state
airflow tasks state aiops_workflow collect_metrics <run_id>
airflow tasks clear aiops_workflow                                # re-run
airflow tasks failed-deps aiops_workflow collect_metrics <run_id>
```

### Config, DB, users
```bash
airflow config list
airflow config get-value core dags_folder
airflow config get-value core executor
airflow info                                     # env diagnostics
airflow db migrate                               # init/upgrade DB
airflow db reset                                 # DANGER: wipes metadata
airflow db check
airflow users create --username admin --firstname A --lastname K --role Admin --email a@b.com   # 2.x / FAB auth manager
```

### Connections, variables, pools
```bash
airflow connections add my_pg --conn-type postgres --conn-host localhost --conn-login u --conn-password p --conn-port 5432 --conn-schema db
airflow connections list
airflow connections delete my_pg

airflow variables set env dev
airflow variables get env
airflow variables list
airflow variables delete env
airflow variables export vars.json
airflow variables import vars.json

airflow pools set heavy 4 "heavy jobs"
airflow pools list
```

### Components
```bash
airflow scheduler
airflow dag-processor
airflow api-server --port 8080
airflow triggerer                                # for deferrable operators
airflow celery worker                            # CeleryExecutor
airflow standalone
```

## 13. Key config (`airflow.cfg` or env `AIRFLOW__SECTION__KEY`)

```bash
export AIRFLOW__CORE__LOAD_EXAMPLES=False              # hide example DAGs
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=32
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@localhost/airflow
export AIRFLOW__API__PORT=8080
```
SQLite is for dev only (no parallel tasks); use Postgres for anything real.

## 14. Project layout

```
airflow/
├── dags/            # DAG files (must be importable, keep top-level code light)
├── plugins/
├── logs/
├── airflow.cfg
└── airflow.db       # SQLite metadata (dev)
```

Rules: no heavy work at DAG top level (it runs on every parse), unique `dag_id`s, use `catchup=False` unless you want backfills.

## 15. Troubleshooting

| Problem | Fix |
|---|---|
| DAG not in UI | `airflow dags list-import-errors`; make sure `dag-processor` is running; check `dags_folder` |
| `ModuleNotFoundError: airflow.operators.python` | Airflow 3: use `airflow.providers.standard.operators.python` (install `apache-airflow-providers-standard`) |
| Tasks stay `queued` | Scheduler not running, or pool/parallelism exhausted |
| Task fails, no logs | Check `~/airflow/logs/dag_id=.../run_id=.../task_id=.../` |
| Edited DAG not updating | Wait for re-parse, or `airflow dags reserialize` |
| Port 8080 busy | `airflow api-server --port 8081` |
| Broken venv path (`python3.14`) | Recreate venv; Airflow supports a limited range of Python versions |
| Debug a task quickly | `airflow tasks test <dag> <task> <date>` |
| Debug a whole DAG in one process | `airflow dags test <dag>` |

## 16. Practical order of operations

```bash
source .venv/bin/activate
airflow config get-value core dags_folder
cp aiops_dag.py ~/airflow/dags/
airflow dags list | grep aiops
airflow dags list-import-errors
airflow dag-processor &
airflow scheduler &
airflow api-server --port 8080          # open http://localhost:8080
airflow dags trigger aiops_workflow
```
