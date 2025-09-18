from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "akshaya",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="binance_daily",
    start_date=datetime(2025, 9, 10),
    schedule="0 2 * * *",     # 02:00 daily (adjust)
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["binance","dlt","postgres"]
) as dag:

    # 1) Run your DLT loader (adjust module/args if needed)
    load = BashOperator(
        task_id="load_dlt",
        cwd="/opt/airflow/app",
        bash_command=(
            'set -euo pipefail; '
            'echo "PWD=$(pwd)"; '
            'python -V; '
            'python -c "import sys; print(\'sys.path[0]=\', sys.path[0])"; '
            'python -c "import binance_pipeline; print(\'binance_pipeline import OK\')"; '
            'python -m binance_pipeline.pipeline'
        ),
        env={"PYTHONPATH": "/opt/airflow/app", **os.environ}
    )

    # 2) Apply SQL transforms to Postgres (runs in file order: 00,10,20,30...)
    apply_sql_transforms = BashOperator(
    task_id="apply_sql_transforms",
    cwd="/opt/airflow/app/sql",
    bash_command=r"""
    {% raw %}
      set -euxo pipefail
      echo "SQL directory: $(pwd)"
      ls -la

      shopt -s nullglob
      files=( *.sql )
      if [ ${#files[@]} -eq 0 ]; then
        echo "No SQL files found in $(pwd). Skipping transforms."
        exit 0
      fi

      # Refresh MV before running
      PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -v ON_ERROR_STOP=1 -c "REFRESH MATERIALIZED VIEW stg.klines_1h;"

      # Run all SQL scripts in order
      for f in "${files[@]}"; do
        echo "== Running $f =="
        PGPASSWORD="$PGPASSWORD" psql \
          -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
          -v ON_ERROR_STOP=1 -f "$f"
      done
    {% endraw %}
    """,
    env={
      "PGHOST": "host.docker.internal",
      "PGPORT": "5433",
      "PGUSER": "dlt_user",
      "PGPASSWORD": "root",
      "PGDATABASE": "binance_dw",
    },
    retries=0,
)

    # 3) Simple DQ check (rowcount > 0 in staging 1h)
    dq = BashOperator(
        task_id="dq_rowcount_stg_1h",
        bash_command=r"""
        {% raw %}
        set -euo pipefail
        # Refresh the materialized view first
        PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
          -v ON_ERROR_STOP=1 -c "REFRESH MATERIALIZED VIEW stg.klines_1h;"

        # Count rows in the materialized view
        ROWCOUNT=$(PGPASSWORD="$PGPASSWORD" psql -t -A \
          -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
          -c "SELECT COUNT(*) FROM stg.klines_1h;")
        echo "Row count in stg.klines_1h: $ROWCOUNT"
        if [ "${ROWCOUNT}" -eq 0 ]; then
          echo "Rowcount=0"
          exit 1
        fi
        {% endraw %}
        """,
        env={
            "PGHOST": "host.docker.internal",
            "PGPORT": "5433",
            "PGUSER": "dlt_user",
            "PGPASSWORD": "root",
            "PGDATABASE": "binance_dw",
        },
        retries=0,
    )

    load >> apply_sql_transforms >> dq