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
        arguments=[
            # TEST 1 : Vérifier si le cluster sort sur Internet
            'echo "--- TEST SORTIE INTERNET ---" && '
            'curl -I https://www.google.com && ' 
            
            # TEST 2 : Vérifier la connectivité brute au port ClickHouse
            'echo "--- TEST PORT CLICKHOUSE ---" && '
            'curl -k -v https://195.154.197.217:8443 2>&1 | grep -E "Connected|refused|timeout" && '
            
            # TEST 3 : Exécution DLT avec les nouveaux paramètres
            'echo "--- TEST DLT PIPELINE ---" && '
            'pip install dlt[clickhouse] && '
            'export DESTINATION__CLICKHOUSE__CREDENTIALS="$AIRFLOW_CONN_CLICKHOUSE_DEFAULT" && '
            'python -c "'
            'import dlt; '
            'data = [{\'status\': \'public_test\', \'timestamp\': \'2026-02-23\'}]; '
            'pipeline = dlt.pipeline(pipeline_name=\'check_scw_public\', destination=\'clickhouse\', dataset_name=\'test_dlt\'); '
            'info = pipeline.run(data, table_name=\'connection_check\'); '
            'print(info)"'
        ],
        env_from=[{"secretRef": {"name": "dwh-connections-secret"}}],
        get_logs=True,
        is_delete_operator_pod=False
    )