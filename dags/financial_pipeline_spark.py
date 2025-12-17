# /opt/airflow/dags/financial_pipeline_spark.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
import logging

logger = logging.getLogger(__name__)

# Import de votre processeur
sys.path.insert(0, '/opt/airflow/processors')
from financial_spark_processor import FinancialSparkProcessor

default_args = {
    'owner': 'soukaina_assia_khalil',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['soukaina@example.com', 'assia@example.com']  # Remplacez
}

def extract_transform_load(**context):
    """Tâche principale ETL avec Spark"""
    
    execution_date = context['execution_date']
    logger.info(f"🚀 DÉMARRAGE PIPELINE SPARK - {execution_date}")
    
    processor = None
    try:
        # 1. INITIALISATION
        processor = FinancialSparkProcessor(
            app_name=f"FinancialETL_{execution_date.strftime('%Y%m%d')}"
        )
        
        # 2. LECTURE DEPUIS SNOWFLAKE
        logger.info("📖 Étape 1: Lecture données depuis Snowflake...")
        
        # Option A: Lire une table spécifique
        raw_data = processor.read_from_snowflake(
            table_name="ALPHA_VANTAGE_DATA",  # Votre table
            schema="RAW_DATA"
        )
        
        # Option B: Ou lire avec une requête
        # query = f"""
        # SELECT * FROM RAW_DATA.ALPHA_VANTAGE_DATA 
        # WHERE date >= '{execution_date - timedelta(days=30)}'
        #   AND date <= '{execution_date}'
        # """
        # raw_data = processor.read_from_snowflake(query=query)
        
        logger.info(f"📊 Données chargées: {raw_data.count()} lignes")
        
        # 3. CALCUL DES INDICATEURS
        logger.info("🧮 Étape 2: Calcul indicateurs techniques...")
        indicators_df = processor.calculate_all_indicators(raw_data)
        
        # 4. ANALYSES SUPPLÉMENTAIRES
        logger.info("🏢 Étape 3: Analyses sectorielles...")
        sector_df = processor.analyze_sectors(raw_data)
        
        logger.info("🔗 Étape 4: Calcul corrélations...")
        correlation_df = processor.calculate_correlations(raw_data)
        
        # 5. ÉCRITURE DANS SNOWFLAKE
        logger.info("💾 Étape 5: Écriture résultats...")
        
        # Table principale des indicateurs
        processor.write_to_snowflake(
            indicators_df, 
            "TECHNICAL_INDICATORS_SPARK",
            schema="PROCESSED_DATA",
            mode="overwrite"
        )
        
        # Analyses sectorielles
        processor.write_to_snowflake(
            sector_df,
            "SECTOR_ANALYSIS",
            schema="PROCESSED_DATA",
            mode="append"  # Historique
        )
        
        # Corrélations
        processor.write_to_snowflake(
            correlation_df,
            "CORRELATION_MATRIX",
            schema="PROCESSED_DATA",
            mode="overwrite"
        )
        
        # 6. SAUVEGARDE DES MÉTRIQUES
        stats = {
            'execution_date': execution_date.isoformat(),
            'raw_records': raw_data.count(),
            'processed_records': indicators_df.count(),
            'sectors_analyzed': sector_df.select("sector").distinct().count(),
            'correlation_pairs': correlation_df.count(),
            'processing_time': datetime.now().isoformat(),
            'team': ['Soukaina El Mahjoubi', 'Assia Boujnah', 'Khalil Fatima']
        }
        
        context['ti'].xcom_push(key='processing_stats', value=stats)
        
        logger.info(f"✅ PIPELINE TERMINÉ: {stats}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ ERREUR PIPELINE: {e}", exc_info=True)
        if processor:
            processor.stop()
        raise
    
    finally:
        if processor:
            processor.stop()

def data_quality_check(**context):
    """Vérification qualité des données"""
    
    try:
        processor = FinancialSparkProcessor(app_name="QualityCheck")
        
        # Vérifier les tables créées
        tables_to_check = [
            ("PROCESSED_DATA", "TECHNICAL_INDICATORS_SPARK"),
            ("PROCESSED_DATA", "SECTOR_ANALYSIS"),
            ("PROCESSED_DATA", "CORRELATION_MATRIX")
        ]
        
        results = {}
        for schema, table in tables_to_check:
            try:
                df = processor.read_from_snowflake(table, schema=schema)
                results[f"{schema}.{table}"] = {
                    'row_count': df.count(),
                    'columns': df.columns,
                    'min_date': df.select(min("date")).collect()[0][0] if 'date' in df.columns else None,
                    'max_date': df.select(max("date")).collect()[0][0] if 'date' in df.columns else None
                }
                logger.info(f"✅ {table}: {df.count()} lignes")
            except Exception as e:
                results[f"{schema}.{table}"] = {'error': str(e)}
        
        processor.stop()
        context['ti'].xcom_push(key='quality_check', value=results)
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Erreur qualité: {e}")
        raise

def generate_report(**context):
    """Génère un rapport final"""
    
    stats = context['ti'].xcom_pull(key='processing_stats', task_ids='spark_etl')
    quality = context['ti'].xcom_pull(key='quality_check', task_ids='quality_check')
    
    report = f"""
    ===========================================
    RAPPORT PIPELINE SPARK SQL - PROJET FINANCE
    ===========================================
    Équipe: Soukaina El Mahjoubi, Assia Boujnah, Khalil Fatima
    Date exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    📊 STATISTIQUES DE TRAITEMENT:
    - Données brutes traitées: {stats.get('raw_records', 0)} lignes
    - Indicateurs calculés: {stats.get('processed_records', 0)} lignes
    - Secteurs analysés: {stats.get('sectors_analyzed', 0)}
    - Paires corrélées: {stats.get('correlation_pairs', 0)}
    
    ✅ VÉRIFICATION QUALITÉ:
    """
    
    if quality:
        for table, info in quality.items():
            if 'row_count' in info:
                report += f"    - {table}: {info['row_count']} lignes ✓\n"
            else:
                report += f"    - {table}: ERREUR - {info.get('error', 'Unknown')}\n"
    
    report += f"""
    🎯 INDICATEURS CALCULÉS:
    1. Moyennes Mobiles (MA 20, 50, 200)
    2. RSI (Relative Strength Index)
    3. MACD (Moving Average Convergence Divergence)
    4. Bandes de Bollinger
    5. ADX simplifié (Directional Movement)
    6. Signaux de trading
    
    📈 VISUALISATIONS DISPONIBLES:
    1. Graphiques chandeliers + Bandes Bollinger
    2. Dashboard indicateurs techniques (RSI, MACD)
    3. Matrice de corrélations
    4. Analyse sectorielle
    
    ===========================================
    TECHNOLOGIES UTILISÉES:
    • Apache Airflow (Orchestration)
    • Apache Spark SQL (Traitement distribué)
    • Snowflake (Data Warehouse)
    • Streamlit (Visualisation)
    ===========================================
    """
    
    logger.info(report)
    
    # Sauvegarder dans un fichier
    with open(f"/tmp/pipeline_report_{datetime.now().strftime('%Y%m%d')}.txt", "w") as f:
        f.write(report)
    
    return report

# DÉFINITION DU DAG
with DAG(
    'financial_spark_pipeline',
    default_args=default_args,
    description='Pipeline ETL Spark SQL pour données financières',
    schedule_interval='0 18 * * 1-5',  # 18h UTC, jours ouvrables
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'finance', 'snowflake', 'streamlit', 'projet']
) as dag:

    start = DummyOperator(task_id='start')
    
    spark_etl = PythonOperator(
        task_id='spark_etl',
        python_callable=extract_transform_load,
    )
    
    quality_check = PythonOperator(
        task_id='quality_check',
        python_callable=data_quality_check,
    )
    
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )
    
    end = DummyOperator(task_id='end')
    
    # Orchestration
    start >> spark_etl >> quality_check >> generate_report_task >> end
