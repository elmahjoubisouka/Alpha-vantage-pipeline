"""
DAG SPARK CORRIGÉ - Version fonctionnelle
"""
import sys  # AJOUTER CET IMPORT!
sys.path.insert(0, '/opt/airflow/processors')  # Ajout explicite

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'soukaina_assia_khalil',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def test_spark_minimal(**context):
    """Tâche SPARK MINIMALE pour test"""
    
    print("=" * 60)
    print("🧪 TEST SPARK MINIMAL DANS AIRFLOW")
    print("=" * 60)
    
    try:
        from pyspark.sql import SparkSession
        
        # 1. Créer session Spark
        spark = SparkSession.builder \
            .appName("AirflowSparkTest") \
            .master("local[1]") \
            .getOrCreate()
        
        print(f"✅ Spark version: {spark.version}")
        
        # 2. Créer des données de test
        data = [
            ("AAPL", "2024-01-01", 185.5, 187.0, 184.0, 186.2, 1000000),
            ("MSFT", "2024-01-01", 370.2, 372.5, 369.0, 371.0, 800000),
        ]
        
        df = spark.createDataFrame(data, ["symbol", "date", "open", "high", "low", "close", "volume"])
        print(f"📊 DataFrame créé: {df.count()} lignes")
        df.show()
        
        # 3. Calcul simple avec Spark SQL
        df.createOrReplaceTempView("stocks")
        
        result = spark.sql("""
            SELECT 
                symbol,
                AVG(close) as avg_price,
                SUM(volume) as total_volume
            FROM stocks
            GROUP BY symbol
        """)
        
        print("📈 Résultat requête SQL:")
        result.show()
        
        # 4. Nettoyer
        spark.stop()
        
        # 5. Sauvegarder résultat
        stats = {
            'test_date': datetime.now().isoformat(),
            'spark_version': spark.version,
            'rows_processed': df.count(),
            'result': [row.asDict() for row in result.collect()]
        }
        
        context['ti'].xcom_push(key='test_results', value=stats)
        
        print("🎉 TEST SPARK RÉUSSI DANS AIRFLOW!")
        return stats
        
    except Exception as e:
        print(f"❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        raise

def test_snowflake_connection(**context):
    """Test connexion Snowflake (optionnel)"""
    
    print("❄️ Test connexion Snowflake...")
    
    try:
        import snowflake.connector
        
        # UTILISEZ VOS CREDENTIALS ICI
        conn = snowflake.connector.connect(
            user='SOUKAINA',
            password='soso123456SOSO123456@',
            account='eqqseml-wv78446',
            warehouse='COMPUTE_WH',
            database='FINANCE_DB',
            schema='RAW_DATA'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP(), CURRENT_VERSION()")
        result = cursor.fetchone()
        
        print(f"✅ Snowflake connecté!")
        print(f"   Time: {result[0]}")
        print(f"   Version: {result[1]}")
        
        cursor.close()
        conn.close()
        
        return {"status": "connected", "timestamp": str(result[0])}
        
    except Exception as e:
        print(f"⚠️ Note: Connexion Snowflake non configurée")
        print(f"   (Pour le projet réel, ajoutez vos credentials)")
        return {"status": "not_tested", "reason": str(e)[:100]}

# DAG PRINCIPAL - VERSION SIMPLIFIÉE
with DAG(
    'financial_spark_pipeline_simple',
    default_args=default_args,
    description='Pipeline Spark simplifié pour le projet',
    schedule_interval='@daily',  # Exécution quotidienne
    catchup=False,
    tags=['spark', 'finance', 'projet']
) as dag:

    start = DummyOperator(task_id='start')
    
    test_spark = PythonOperator(
        task_id='test_spark_operation',
        python_callable=test_spark_minimal,
    )
    
    test_snowflake = PythonOperator(
        task_id='test_snowflake_connection',
        python_callable=test_snowflake_connection,
    )
    
    end = DummyOperator(task_id='end')
    
    # Orchestration
    start >> test_spark >> test_snowflake >> end

print("✅ DAG 'financial_spark_pipeline_simple' créé avec succès!")
