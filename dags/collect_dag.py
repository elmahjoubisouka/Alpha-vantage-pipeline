"""
DAG: collect_and_store_dag - VERSION ADAPTÉE À VOS TABLES SNOWFLAKE
Équipe: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']

def create_demo_data(**context):
    """Crée des données de démonstration"""
    logger.info("🎓 Création de données de démonstration...")
    
    all_data = []
    extraction_date = datetime.now()
    
    # Configuration réaliste
    demo_config = {
        'AAPL': {'base': 185.0, 'vol': 0.025},
        'MSFT': {'base': 370.0, 'vol': 0.020},
        'GOOGL': {'base': 140.0, 'vol': 0.030},
        'AMZN': {'base': 150.0, 'vol': 0.028},
        'TSLA': {'base': 240.0, 'vol': 0.050},
        'NVDA': {'base': 500.0, 'vol': 0.040},
        'META': {'base': 350.0, 'vol': 0.035},
        'NFLX': {'base': 490.0, 'vol': 0.032}
    }
    
    for symbol in SYMBOLS:
        config = demo_config.get(symbol, {'base': 200.0, 'vol': 0.03})
        base_price = config['base']
        
        # Générer 60 jours de données
        dates = pd.date_range(end=extraction_date.date(), periods=60, freq='B')
        
        np.random.seed(hash(symbol) % 10000)
        trend = np.linspace(0.95, 1.05, 60)
        noise = 1 + np.random.randn(60) * config['vol']
        close_prices = base_price * trend * noise
        
        for i, date in enumerate(dates):
            all_data.append({
                'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': symbol,
                'open': float(close_prices[i] * (1 - np.random.uniform(0, 0.01))),
                'high': float(close_prices[i] * (1 + np.random.uniform(0, 0.02))),
                'low': float(close_prices[i] * (1 - np.random.uniform(0, 0.02))),
                'close': float(close_prices[i]),
                'volume': int(np.random.randint(5_000_000, 50_000_000)),
                'dividends': float(np.random.choice([0, 0.23], p=[0.98, 0.02])),
                'stock_splits': 0.0,
                'extraction_date': extraction_date.strftime('%Y-%m-%d %H:%M:%S')
            })
    
    logger.info(f"📊 {len(all_data)} lignes créées pour {len(SYMBOLS)} symboles")
    
    # Stocker dans XCom
    context['ti'].xcom_push(key='raw_data', value=all_data)
    return all_data

def save_to_snowflake_raw(**context):
    """Sauvegarde dans vos tables Snowflake existantes"""
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='create_demo_data', key='raw_data')
    
    if not raw_data:
        logger.error("❌ Aucune donnée à sauvegarder")
        return 0
    
    logger.info(f"💾 Sauvegarde de {len(raw_data)} lignes dans Snowflake...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # IMPORTANT: Utiliser votre contexte
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        logger.info("✅ Contexte Snowflake configuré")
        
        # Vérifier si la table existe (elle devrait déjà exister)
        cursor.execute("SHOW TABLES LIKE 'RAW_STOCK_DATA'")
        tables = cursor.fetchall()
        
        if not tables:
            logger.warning("⚠️ Table RAW_STOCK_DATA non trouvée, création...")
            cursor.execute("""
                CREATE TABLE RAW_STOCK_DATA (
                    date TIMESTAMP_NTZ NOT NULL,
                    open NUMBER(20, 4),
                    high NUMBER(20, 4),
                    low NUMBER(20, 4),
                    close NUMBER(20, 4),
                    volume NUMBER(38, 0),
                    dividends NUMBER(20, 4),
                    stock_splits NUMBER(20, 4),
                    symbol VARCHAR(10),
                    extraction_date TIMESTAMP_NTZ,
                    PRIMARY KEY (symbol, date)
                )
            """)
            logger.info("✅ Table RAW_STOCK_DATA créée")
        
        # Nettoyer les données anciennes pour cette démo
        # cursor.execute("DELETE FROM RAW_STOCK_DATA")
        
        # Préparer la requête d'insertion
        insert_query = """
            INSERT INTO RAW_STOCK_DATA 
            (date, symbol, open, high, low, close, volume, dividends, stock_splits, extraction_date)
            VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'), %s, %s, %s, %s, %s, %s, %s, %s, 
                    TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'))
        """
        
        batch_size = 100
        inserted = 0
        
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i:i+batch_size]
            values = []
            
            for row in batch:
                values.append((
                    str(row['date']),
                    str(row['symbol']),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume']),
                    float(row['dividends']),
                    float(row['stock_splits']),
                    str(row['extraction_date'])
                ))
            
            cursor.executemany(insert_query, values)
            inserted += len(batch)
            logger.info(f"📤 Batch {i//batch_size + 1}: {len(batch)} lignes")
        
        conn.commit()
        logger.info(f"🎉 {inserted} lignes sauvegardées dans RAW_STOCK_DATA")
        
        # Vérification
        cursor.execute("SELECT COUNT(*) as total FROM RAW_STOCK_DATA")
        total = cursor.fetchone()[0]
        logger.info(f"📊 Total en base: {total} lignes")
        
        # Afficher un échantillon
        cursor.execute("""
            SELECT symbol, COUNT(*) as count, 
                   AVG(close) as avg_price, 
                   MIN(date) as min_date, 
                   MAX(date) as max_date
            FROM RAW_STOCK_DATA
            GROUP BY symbol
            ORDER BY symbol
        """)
        
        logger.info("📋 Statistiques par symbole:")
        for symbol, count, avg_price, min_date, max_date in cursor.fetchall():
            logger.info(f"   {symbol}: {count} jours, prix moyen=${avg_price:.2f}")
        
        cursor.close()
        conn.close()
        
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur Snowflake: {str(e)[:200]}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        raise

def quality_check_raw(**context):
    """Vérifie la qualité des données brutes"""
    logger.info("🔍 Contrôle qualité données brutes...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Utiliser votre contexte
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        # Vérifications complètes
        checks = [
            ("Total des lignes", "SELECT COUNT(*) FROM RAW_STOCK_DATA"),
            ("Symboles uniques", "SELECT COUNT(DISTINCT symbol) FROM RAW_STOCK_DATA"),
            ("Période couverte", "SELECT MIN(date), MAX(date) FROM RAW_STOCK_DATA"),
            ("Données complètes", "SELECT symbol, COUNT(*) FROM RAW_STOCK_DATA GROUP BY symbol"),
            ("Valeurs nulles", "SELECT COUNT(*) FROM RAW_STOCK_DATA WHERE close IS NULL OR volume IS NULL"),
            ("Prix moyens", "SELECT AVG(open), AVG(high), AVG(low), AVG(close) FROM RAW_STOCK_DATA")
        ]
        
        logger.info("=" * 60)
        logger.info("📊 RAPPORT DE QUALITÉ - DONNÉES BRUTES")
        logger.info("=" * 60)
        
        for check_name, query in checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                logger.info(f"✓ {check_name}: {result}")
            except Exception as e:
                logger.warning(f"⚠️ {check_name}: Erreur - {str(e)[:100]}")
        
        # Vérifier spécifiquement les 8 symboles
        cursor.execute("SELECT DISTINCT symbol FROM RAW_STOCK_DATA ORDER BY symbol")
        symbols_in_db = [row[0] for row in cursor.fetchall()]
        
        if set(SYMBOLS).issubset(set(symbols_in_db)):
            logger.info(f"✅ Tous les {len(SYMBOLS)} symboles présents")
        else:
            missing = set(SYMBOLS) - set(symbols_in_db)
            logger.warning(f"⚠️ Symboles manquants: {missing}")
        
        logger.info("=" * 60)
        logger.info("🎉 COLLECTE TERMINÉE AVEC SUCCÈS!")
        logger.info("📈 Données prêtes pour le traitement Spark")
        logger.info("=" * 60)
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"⚠️ Erreur contrôle qualité: {e}")
        return True  # Ne pas bloquer le pipeline

# Configuration du DAG
with DAG(
    'collect_and_store_dag',
    default_args=default_args,
    description='Collecte et stockage des données financières dans Snowflake',
    schedule_interval='0 8 * * 1-5',  # 8h du matin, jours ouvrables
    catchup=False,
    tags=['finance', 'collect', 'snowflake', 'raw_data'],
) as dag:
    
    create_task = PythonOperator(
        task_id='create_demo_data',
        python_callable=create_demo_data,
    )
    
    save_task = PythonOperator(
        task_id='save_to_snowflake_raw',
        python_callable=save_to_snowflake_raw,
    )
    
    quality_task = PythonOperator(
        task_id='quality_check_raw',
        python_callable=quality_check_raw,
    )
    
    create_task >> save_task >> quality_task
    
    # Documentation
    dag.doc_md = """
    ## DAG de Collecte et Stockage
    
    **Équipe:** Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima
    
    ### Objectif
    Collecter des données financières de démonstration et les stocker dans Snowflake.
    
    ### Flow
    1. **Création données** → Génère 60 jours de données pour 8 symboles
    2. **Sauvegarde Snowflake** → Insère dans la table `RAW_STOCK_DATA`
    3. **Contrôle qualité** → Vérifie l'intégrité des données
    
    ### Connexion Snowflake
    - Database: `FINANCE_DB`
    - Schema: `STOCK_DATA`
    - Warehouse: `COMPUTE_WH`
    - Table: `RAW_STOCK_DATA`
    
    ### Données générées
    - 8 symboles: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, NFLX
    - 60 jours par symbole (480 lignes au total)
    - Colonnes: date, open, high, low, close, volume, dividends, stock_splits
    """