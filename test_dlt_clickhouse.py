from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id='test_dlt_connection',
    start_date=datetime(2026, 2, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    run_dlt = KubernetesPodOperator(
        task_id="check_clickhouse_connection",
        name="dlt-test-pod",
        namespace="airflow",
        image="python:3.11-slim",
        cmds=["bash", "-cx"],
        arguments=[
            "pip install dlt[clickhouse] && python -c '"
            "import dlt; "
            "data = [{\"status\": \"works\", \"env\": \"k8s\", \"timestamp\": \"2026-02-20\"}]; "
            "pipeline = dlt.pipeline(pipeline_name=\"check_scw\", destination=\"clickhouse\", dataset_name=\"test_dlt\"); "
            "info = pipeline.run(data, table_name=\"connection_check\"); "
            "print(info)'"
        ],
        # On injecte le secret ClickHouse spécifiquement dans ce Pod
        env_from=[{
            "secretRef": {
                "name": "dwh-connections-secret"
            }
        }],
        get_logs=True,
        is_delete_operator_pod=False # On le garde pour voir les logs en cas d'erreur
    )