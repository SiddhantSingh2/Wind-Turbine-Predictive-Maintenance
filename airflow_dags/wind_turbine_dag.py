from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'wind_turbine_pipeline',
    default_args=default_args,
    description='End-to-end wind turbine predictive maintenance pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['wind_turbine', 'scada', 'predictive_maintenance'],
) as dag:

    # 1. Processing (PySpark)
    process_edp_data = BashOperator(
        task_id='process_edp_scada_data',
        bash_command='python /path/to/scripts/edp_processing.py',
    )

    # 2. Transformations (dbt)
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --profiles-dir /path/to/dbt_profile',
    )

    # 3. Data Quality Checks (dbt)
    test_dbt_models = BashOperator(
        task_id='test_dbt_models',
        bash_command='dbt test --profiles-dir /path/to/dbt_profile',
    )

    process_edp_data >> run_dbt_models >> test_dbt_models
