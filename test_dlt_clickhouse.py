from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id="test_dlt_connection",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    run_dlt = KubernetesPodOperator(
        task_id="check_clickhouse_connection",
        name="dlt-test-pod",
        namespace="airflow",
        image="python:3.11-slim",
        cmds=["bash", "-cx"],
        arguments=[
            # 1. On installe les dépendances
            # 2. On transforme la variable Airflow en variable DLT au runtime
            "pip install dlt[clickhouse] && "
            "export DESTINATION__CLICKHOUSE__CREDENTIALS=$AIRFLOW_CONN_CLICKHOUSE_DEFAULT && "
            "python -c '"
            "import dlt; "
            "import os; "
            "data = [{\"status\": \"works\", \"env\": \"k8s\", \"timestamp\": \"2026-02-22\"}]; "
            "pipeline = dlt.pipeline(pipeline_name=\"check_scw\", destination=\"clickhouse\", dataset_name=\"test_dlt\"); "
            "info = pipeline.run(data, table_name=\"connection_check\"); "
            "print(info)'"
        ],
        env_from=[{
            "secretRef": {
                "name": "dwh-connections-secret"
            }
        }],
        get_logs=True,
        is_delete_operator_pod=False 
    )