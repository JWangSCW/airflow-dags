from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# Ce bloc est INDISPENSABLE pour qu'Airflow voit le DAG
with DAG(
    dag_id='1_seed_postgres_source',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['setup']
) as dag:

    # On appelle le secret créé via Terraform
    env_from_postgres = [
        k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name='pg-connection-secret')
        )
    ]

    setup_tables = KubernetesPodOperator(
        task_id='create_and_populate_ecommerce',
        name='init-db-pod',
        namespace='airflow',
        image='postgres:15',
        env_from=env_from_postgres,
        cmds=["bash", "-cx"],
        arguments=[
            'psql "$AIRFLOW_CONN_POSTGRES_SOURCE" -c "'
            'CREATE SCHEMA IF NOT EXISTS ecommerce; '
            'CREATE TABLE IF NOT EXISTS ecommerce.users (id SERIAL PRIMARY KEY, name TEXT); '
            'INSERT INTO ecommerce.users (name) VALUES (\'Alice\'), (\'Bob\');"'
        ],
        is_delete_operator_pod=True,
        get_logs=True
    )