from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'finance_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'test_snowflake_connection',
    default_args=default_args,
    description='Test de connexion à Snowflake',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'snowflake'],
) as dag:
    
    # Tâche 1 : Créer une table test
    create_table = SnowflakeOperator(
        task_id='create_test_table',
        sql="""
        CREATE OR REPLACE TABLE finance_db.stock_data.test_airflow (
            id INT,
            test_name STRING,
            created_at TIMESTAMP_NTZ
        )
        """,
        snowflake_conn_id='snowflake_default',
    )
    
    # Tâche 2 : Insérer des données
    insert_data = SnowflakeOperator(
        task_id='insert_test_data',
        sql="""
        INSERT INTO finance_db.stock_data.test_airflow VALUES
        (1, 'Test Airflow Connection', CURRENT_TIMESTAMP()),
        (2, 'Projet Finance ENSA Safi', CURRENT_TIMESTAMP()),
        (3, 'Équipe: Assia, Soukaina, Khalil', CURRENT_TIMESTAMP())
        """,
        snowflake_conn_id='snowflake_default',
    )
    
    # Tâche 3 : Lire les données
    select_data = SnowflakeOperator(
        task_id='select_test_data',
        sql="SELECT * FROM finance_db.stock_data.test_airflow",
        snowflake_conn_id='snowflake_default',
    )
    
    # Tâche 4 : Nettoyer
    drop_table = SnowflakeOperator(
        task_id='drop_test_table',
        sql="DROP TABLE IF EXISTS finance_db.stock_data.test_airflow",
        snowflake_conn_id='snowflake_default',
    )
    
    create_table >> insert_data >> select_data >> drop_table