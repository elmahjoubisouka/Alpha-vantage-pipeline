# dags/financial_spark_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
import logging

from processors.spark_processor import SparkFinanceProcessor

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_with_spark(**context):
    """Tâche principale de traitement Spark"""
    
    logger.info("🚀 DÉMARRAGE TRAITEMENT SPARK SQL")
    logger.info(f"Exécution: {context['execution_date']}")
    
    try:
        # Initialiser le processeur Spark
        processor = SparkFinanceProcessor(app_name="Airflow_Financial_ETL")
        
        # 1. Lire les données depuis Snowflake
        logger.info("Étape 1: Lecture données Snowflake...")
        raw_data = processor.read_from_snowflake("ALPHA_VANTAGE_DATA")
        
        # Vérifier les données
        if raw_data.count() == 0:
            raise ValueError("Aucune donnée à traiter")
        
        logger.info(f"📊 Données chargées: {raw_data.count()} lignes")
        logger.info(f"Colonnes: {', '.join(raw_data.columns)}")
        
        # 2. Calculer les indicateurs techniques
        logger.info("Étape 2: Calcul indicateurs techniques...")
        indicators_df = processor.calculate_technical_indicators(raw_data)
        
        # 3. Écrire les indicateurs dans Snowflake
        logger.info("Étape 3: Écriture indicateurs Snowflake...")
        processor.write_to_snowflake(
            indicators_df, 
            "TECHNICAL_INDICATORS_SPARK", 
            schema="RAW_DATA",
            mode="overwrite"
        )
        
        # 4. Calculer les corrélations (démonstration avancée)
        logger.info("Étape 4: Calcul corrélations...")
        correlations_df = processor.calculate_correlations(raw_data)
        
        # 5. Écrire les corrélations
        processor.write_to_snowflake(
            correlations_df,
            "CORRELATIONS_SPARK",
            schema="RAW_DATA",
            mode="overwrite"
        )
        
        # 6. Générer rapport mensuel
        logger.info("Étape 5: Génération rapport mensuel...")
        monthly_report = processor.generate_monthly_report(raw_data)
        
        processor.write_to_snowflake(
            monthly_report,
            "MONTHLY_REPORT_SPARK",
            schema="RAW_DATA",
            mode="overwrite"
        )
        
        # 7. Arrêter Spark
        processor.stop()
        
        # Sauvegarder les statistiques dans XCom
        stats = {
            'raw_rows': raw_data.count(),
            'indicators_rows': indicators_df.count(),
            'correlations_count': correlations_df.count(),
            'monthly_reports': monthly_report.count(),
            'execution_time': datetime.now().isoformat()
        }
        
        context['ti'].xcom_push(key='spark_stats', value=stats)
        
        logger.info("✅ TRAITEMENT SPARK TERMINÉ AVEC SUCCÈS")
        logger.info(f"Statistiques: {stats}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ ERREUR TRAITEMENT SPARK: {e}")
        
        # Arrêter Spark en cas d'erreur
        if 'processor' in locals():
            processor.stop()
        
        raise

def verify_spark_results(**context):
    """Vérifie les résultats du traitement Spark"""
    
    try:
        processor = SparkFinanceProcessor(app_name="Verification")
        
        # Vérifier les tables créées
        tables_to_check = [
            "TECHNICAL_INDICATORS_SPARK",
            "CORRELATIONS_SPARK", 
            "MONTHLY_REPORT_SPARK"
        ]
        
        results = {}
        
        for table in tables_to_check:
            try:
                df = processor.read_from_snowflake(table)
                results[table] = {
                    'row_count': df.count(),
                    'columns': len(df.columns),
                    'sample': df.limit(3).collect()
                }
                logger.info(f"✅ {table}: {df.count()} lignes")
            except Exception as e:
                results[table] = {'error': str(e)}
                logger.error(f"❌ {table}: {e}")
        
        processor.stop()
        
        context['ti'].xcom_push(key='verification_results', value=results)
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Erreur vérification: {e}")
        raise

def generate_report(**context):
    """Génère un rapport final"""
    
    spark_stats = context['ti'].xcom_pull(key='spark_stats', task_ids='process_with_spark')
    verification = context['ti'].xcom_pull(key='verification_results', task_ids='verify_spark_results')
    
    logger.info("=" * 60)
    logger.info("📋 RAPPORT FINAL - TRAITEMENT SPARK SQL")
    logger.info("=" * 60)
    logger.info("ÉQUIPE: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima")
    logger.info(f"Date exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if spark_stats:
        logger.info("📊 STATISTIQUES DE TRAITEMENT:")
        logger.info(f"  Lignes brutes traitées: {spark_stats.get('raw_rows', 0)}")
        logger.info(f"  Indicateurs calculés: {spark_stats.get('indicators_rows', 0)}")
        logger.info(f"  Corrélations: {spark_stats.get('correlations_count', 0)}")
        logger.info(f"  Rapports mensuels: {spark_stats.get('monthly_reports', 0)}")
    
    if verification:
        logger.info("✅ VÉRIFICATION DES RÉSULTATS:")
        for table, stats in verification.items():
            if 'row_count' in stats:
                logger.info(f"  {table}: {stats['row_count']} lignes - OK")
            else:
                logger.warning(f"  {table}: ERREUR - {stats.get('error', 'Unknown')}")
    
    logger.info("🎯 TECHNOLOGIES UTILISÉES:")
    logger.info("  • Apache Spark SQL: Traitement distribué")
    logger.info("  • PySpark: Interface Python pour Spark")
    logger.info("  • Spark-Snowflake Connector: Intégration Snowflake")
    logger.info("  • Window Functions: Calculs analytiques")
    logger.info("  • Airflow: Orchestration du pipeline")
    
    logger.info("=" * 60)
    
    return "Rapport généré"

with DAG(
    'financial_spark_pipeline',
    default_args=default_args,
    description='Pipeline ETL avec Spark SQL pour données financières',
    schedule_interval='0 22 * * 1-5',  # 22h UTC, jours ouvrables
    catchup=False,
    tags=['spark', 'financial', 'etl', 'snowflake', 'projet']
) as dag:

    start = DummyOperator(task_id='start')
    
    spark_processing = PythonOperator(
        task_id='process_with_spark',
        python_callable=process_with_spark,
    )
    
    verification = PythonOperator(
        task_id='verify_spark_results',
        python_callable=verify_spark_results,
    )
    
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )
    
    end = DummyOperator(task_id='end')
    
    # Orchestration
    start >> spark_processing >> verification >> report >> end