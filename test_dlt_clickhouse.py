from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

# with DAG(
#     dag_id="test_dlt_connection",
#     schedule="@daily",
#     start_date=datetime(2024, 1, 1),
#     catchup=False
# ) as dag:
with DAG(
    dag_id='test_dlt_connection',
    schedule='@once',  # ou None
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
        # arguments=[
        #     # On regroupe tout dans une seule chaîne Bash pour garantir l'ordre
        #     'curl -k -v https://scwadmin:Xingyun2026!@195.154.197.217:8443 && ' # -k pour ignorer SSL si besoin
        #     'pip install dlt[clickhouse] && '
        #     'export DESTINATION__CLICKHOUSE__CREDENTIALS="$AIRFLOW_CONN_CLICKHOUSE_DEFAULT" && '
        #     'python -c "'
        #     'import dlt; '
        #     'data = [{\'status\': \'works\', \'env\': \'k8s\', \'timestamp\': \'2026-02-22\'}]; '
        #     'pipeline = dlt.pipeline(pipeline_name=\'check_scw\', destination=\'clickhouse\', dataset_name=\'test_dlt\'); '
        #     'info = pipeline.run(data, table_name=\'connection_check\'); '
        #     'print(info)"'
        # ],
        arguments=[
            'curl -v http://c2157035-def6-4dcb-bb99-47452fd8dd68.9b171428-b846-44b5-b93d-28b29b57a7c8.internal:8123 && '
            'pip install dlt[clickhouse] && '
            'export DESTINATION__CLICKHOUSE__CREDENTIALS="$AIRFLOW_CONN_CLICKHOUSE_DEFAULT" && '
            'python -c "'
            'import dlt; '
            'data = [{\'status\': \'private_works\', \'env\': \'kapsule_pn\'}]; '
            'pipeline = dlt.pipeline(pipeline_name=\'check_scw_pn\', destination=\'clickhouse\', dataset_name=\'test_dlt\'); '
            'info = pipeline.run(data, table_name=\'connection_check\'); '
            'print(info)"'
        ],
        env_from=[{"secretRef": {"name": "dwh-connections-secret"}}],
        get_logs=True,
        is_delete_operator_pod=False
    )