# dags/binance_backfill.py
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="binance_backfill",
    start_date=datetime(2025, 9, 10),
    schedule=None,
    catchup=False,
    tags=["binance","backfill"]
) as dag:

    backfill = BashOperator(
        task_id="backfill",
        cwd="/opt/airflow/app",
        bash_command=(
            "python -m binance_pipeline.pipeline "
            "--symbol '{{ dag_run.conf.get(\"symbol\", \"BTCUSDT\") }}' "
            "--interval '{{ dag_run.conf.get(\"interval\", \"1h\") }}' "
            "--start '{{ dag_run.conf.get(\"start\") }}' "
            "--end   '{{ dag_run.conf.get(\"end\") }}'"
        ),
        env=os.environ
    )