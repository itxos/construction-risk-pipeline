import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PY = "python /opt/airflow/ingest/ingest_osha.py"
DBT = "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt"


@dag(
    dag_id="osha_construction_daily",
    schedule="0 11 * * *",  # 11:00 UTC, early morning in Texas
    start_date=pendulum.datetime(2026, 10, 21, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["osha", "construction"],
)
def osha_construction_daily():
    load_new = BashOperator(
        task_id="load_new_inspections",
        bash_command=PY + " --viol-batch 100",
        skip_on_exit_code=2,  # exit 2 = rate limited; progress is saved
        execution_timeout=pendulum.duration(hours=2),
    )

    refresh = BashOperator(
        task_id="refresh_recent_violations",
        bash_command=PY + " --viol-batch 100 --refresh-since {{ macros.ds_add(ds, -270) }}",
        skip_on_exit_code=2,
        trigger_rule="none_failed",
        execution_timeout=pendulum.duration(hours=2),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run",
        trigger_rule="none_failed",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test",
    )

    load_new >> refresh >> dbt_run >> dbt_test


osha_construction_daily()
