from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime
from airflow import DAG

with DAG(
    dag_id='2_etl_postgres_to_clickhouse',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # On passe nos deux secrets (Source et Destination)
    env_from = [k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='dwh-connections-secret'))]

    run_dlt_pipeline = KubernetesPodOperator(
        task_id='run_dlt_ingestion',
        name='dlt-ingestion-pod',
        namespace='airflow',
        image='python:3.11',
        env_from=env_from,
        cmds=["bash", "-cx"],
        arguments=[
            # Installation des dépendances (Postgres + ClickHouse)
            'pip install dlt[clickhouse] psycopg2-binary && '
            # Lancement du script dlt
            'python -c "'
            'import dlt; '
            'from dlt.sources.sql_database import sql_database; '
            'import os; '
            # 1. Définir la source (Postgres)
            'source = sql_database(os.getenv(\'AIRFLOW_CONN_POSTGRES_SOURCE\'), schema=\'ecommerce\'); '
            # 2. Configurer le pipeline vers ClickHouse
            'pipeline = dlt.pipeline(pipeline_name=\'pg_to_ch\', destination=\'clickhouse\', dataset_name=\'raw_ecommerce\', credentials=os.getenv(\'AIRFLOW_CONN_CLICKHOUSE_DEFAULT\')); '
            # 3. Exécuter
            'info = pipeline.run(source); '
            'print(info)"'
        ],
        get_logs=True,
        is_delete_operator_pod=True
    )