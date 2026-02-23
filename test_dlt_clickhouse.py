from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime


with DAG(
    dag_id='test_dlt_connection',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

        run_dlt = KubernetesPodOperator(
        task_id="check_clickhouse_connection",
        name="dlt-test-pod",
        namespace="airflow",
        # 1. Image complète (pas slim) pour éviter les erreurs de compilation
        image="python:3.11", 
        cmds=["bash", "-cx"],
        arguments=[
            'pip install dlt[clickhouse] && '
            'export DESTINATION__CLICKHOUSE__CREDENTIALS="$AIRFLOW_CONN_CLICKHOUSE_DEFAULT" && '
            'python -c "'
            'import dlt; '
            'data = [{\'status\': \'public_test_https\', \'timestamp\': \'2026-02-23\'}]; '
            'pipeline = dlt.pipeline(pipeline_name=\'check_scw_public\', destination=\'clickhouse\', dataset_name=\'test_dlt\'); '
            'info = pipeline.run(data, table_name=\'connection_check\'); '
            'print(info)"'
        ],
        env_from=[{"secretRef": {"name": "dwh-connections-secret"}}],
        get_logs=True,
        is_delete_operator_pod=False
    )