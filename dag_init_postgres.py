from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

with DAG(
    dag_id='1_seed_postgres_source',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # On injecte SPÉCIFIQUEMENT le nouveau secret
    env_from_postgres = [
        k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name='postgres-connection-secret')
        )
    ]

    seed_db = KubernetesPodOperator(
        task_id='create_and_populate_ecommerce',
        name='init-postgres-pod',
        namespace='airflow',
        image='postgres:15', # Image légère contenant 'psql'
        env_from=env_from_postgres,
        cmds=["bash", "-cx"],
        arguments=[
            # On utilise la variable d'env injectée par le secret
            'psql "$AIRFLOW_CONN_POSTGRES_SOURCE" -c "'
            'CREATE SCHEMA IF NOT EXISTS ecommerce; '
            'CREATE TABLE IF NOT EXISTS ecommerce.users (id SERIAL PRIMARY KEY, name TEXT, email TEXT); '
            'INSERT INTO ecommerce.users (name, email) VALUES (\'Alice\', \'alice@example.com\'), (\'Bob\', \'bob@example.com\');"'
        ],
        is_delete_operator_pod=True, # Le pod sera supprimé, mais on aura eu le temps de voir les logs
        get_logs=True
    )