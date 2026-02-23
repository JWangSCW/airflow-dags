from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='1_seed_postgres_source',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['setup']
) as dag:

    seed_data = SQLExecuteQueryOperator(
        task_id='create_and_populate_ecommerce',
        conn_id='POSTGRES_SOURCE',
        sql="""
            CREATE SCHEMA IF NOT EXISTS ecommerce;
            
            CREATE TABLE IF NOT EXISTS ecommerce.users (
                id SERIAL PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ecommerce.orders (
                id SERIAL PRIMARY KEY,
                user_id INT,
                amount DECIMAL(10,2),
                status TEXT,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            TRUNCATE ecommerce.users, ecommerce.orders RESTART IDENTITY;

            INSERT INTO ecommerce.users (name, email) VALUES 
            ('Alice Smith', 'alice@example.com'), ('Bob Jones', 'bob@example.com'), ('Charlie Brown', 'charlie@example.com');

            INSERT INTO ecommerce.orders (user_id, amount, status) VALUES 
            (1, 150.00, 'shipped'), (1, 25.50, 'pending'), (2, 300.00, 'shipped'), (3, 10.00, 'cancelled');
        """
    )