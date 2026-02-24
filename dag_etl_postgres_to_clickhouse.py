from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

with DAG(
    dag_id='2_etl_postgres_to_clickhouse',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'dlt']
) as dag:

    env_from = [
        k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name='pg-connection-secret')
        ),
        k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name='dwh-connection-secret')
        )
    ]

    run_dlt_ingestion = KubernetesPodOperator(
        task_id='run_dlt_postgres_to_clickhouse',
        name='dlt-ingestion-pod',
        namespace='airflow',
        image='python:3.11-slim',
        env_from=env_from,
        cmds=["bash", "-cx"],
        arguments=[
            'pip install "dlt[clickhouse,sql_database]" psycopg2-binary && '
            'python -c "'
            'import dlt; '
            'from dlt.sources.sql_database import sql_database; '
            'import os; '
            # 1. Chargement de la source
            'source = sql_database(os.getenv(\'AIRFLOW_CONN_POSTGRES_SOURCE\'), schema=\'ecommerce\'); '
            # 2. CORRECTION SYNTAXE : On passe credentials via dlt.destinations.clickhouse()
            'pipeline = dlt.pipeline('
            '    pipeline_name=\'pg_to_ch\', '
            '    destination=dlt.destinations.clickhouse(credentials=os.getenv(\'AIRFLOW_CONN_CLICKHOUSE_DEFAULT\')), '
            '    dataset_name=\'raw_data\''
            '); '
            'load_info = pipeline.run(source); '
            'print(load_info)"'
        ],
        get_logs=True,
        is_delete_operator_pod=True
    )