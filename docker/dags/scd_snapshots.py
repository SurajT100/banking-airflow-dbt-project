from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

    clean_target = BashOperator(
        task_id="clean_dbt_target",
        bash_command="rm -rf /opt/airflow/banking_dbt/target/",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt snapshot --full-refresh --no-partial-parse --profiles-dir /home/airflow/.dbt"
        )
    )

    dbt_run_dim_customers = BashOperator(
        task_id="dbt_run_dim_customers",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt run -s dim_customers --no-partial-parse --profiles-dir /home/airflow/.dbt"
        )
    )

    dbt_run_dim_accounts = BashOperator(
        task_id="dbt_run_dim_accounts",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt run -s dim_accounts --no-partial-parse --profiles-dir /home/airflow/.dbt"
        )
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt run --no-partial-parse --profiles-dir /home/airflow/.dbt"
        )
    )

    # dim_customers and dim_accounts run in parallel after snapshot
    clean_target >> dbt_run_staging >> dbt_snapshot >> [dbt_run_dim_customers, dbt_run_dim_accounts]