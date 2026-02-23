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
            'pip install "dlt[clickhouse,sql_database]" psycopg2-binary && '
            'python -c "'
            'import dlt; '
            'from dlt.sources.sql_database import sql_database; '
            'import os; '
            # Conversion propre pour SQLAlchemy sans toucher au secret K8s
            'pg_url = os.getenv(\'AIRFLOW_CONN_POSTGRES_SOURCE\').replace(\'postgres://\', \'postgresql://\'); '
            'source = sql_database(pg_url, schema=\'ecommerce\'); '
            # Configuration correcte du pipeline dlt
            'pipeline = dlt.pipeline('
            '    pipeline_name=\'pg_to_ch\', '
            '    destination=dlt.destinations.clickhouse(credentials=os.getenv(\'AIRFLOW_CONN_CLICKHOUSE_DEFAULT\')), '
            '    dataset_name=\'raw_data\''
            '); '
            'load_info = pipeline.run(source); '
            'print(load_info)"'
        ],
        env_from=[{"secretRef": {"name": "dwh-connections-secret"}}],
        get_logs=True,
        is_delete_operator_pod=False
    )