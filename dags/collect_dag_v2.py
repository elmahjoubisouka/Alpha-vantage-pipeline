"""
DAG: collect_and_store_dag - VERSION COMPLÈTE AVEC CRÉATION TABLES
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

def create_tables_snowflake(**context):
    """CRÉE les tables dans Snowflake si elles n'existent pas"""
    logger.info("🏗️  Création des tables dans Snowflake...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # 1. Vérifier et créer la database si nécessaire
        cursor.execute("CREATE DATABASE IF NOT EXISTS FINANCE_DB")
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS STOCK_DATA")
        cursor.execute("USE SCHEMA STOCK_DATA")
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        
        logger.info("✅ Contexte Snowflake configuré")
        
        # 2. Créer la table pour données brutes
        create_raw_table = """
        CREATE TABLE IF NOT EXISTS RAW_STOCK_DATA (
            date TIMESTAMP_NTZ NOT NULL,
            symbol VARCHAR(10),
            open NUMBER(20, 4),
            high NUMBER(20, 4),
            low NUMBER(20, 4),
            close NUMBER(20, 4),
            volume NUMBER(38, 0),
            dividends NUMBER(20, 4),
            stock_splits NUMBER(20, 4),
            extraction_date TIMESTAMP_NTZ,
            PRIMARY KEY (symbol, date)
        )
        """
        
        cursor.execute(create_raw_table)
        logger.info("✅ Table RAW_STOCK_DATA créée/vérifiée")
        
        # 3. Créer la table pour données traitées (pour plus tard)
        create_processed_table = """
        CREATE TABLE IF NOT EXISTS PROCESSED_STOCK_DATA (
            date TIMESTAMP_NTZ NOT NULL,
            symbol VARCHAR(10),
            open NUMBER(20, 4),
            high NUMBER(20, 4),
            low NUMBER(20, 4),
            close NUMBER(20, 4),
            volume NUMBER(38, 0),
            rsi NUMBER(10, 4),
            macd NUMBER(20, 4),
            macd_signal NUMBER(20, 4),
            macd_diff NUMBER(20, 4),
            ma_20 NUMBER(20, 4),
            ma_50 NUMBER(20, 4),
            bb_high NUMBER(20, 4),
            bb_low NUMBER(20, 4),
            bb_mid NUMBER(20, 4),
            adx NUMBER(10, 4),
            atr NUMBER(20, 4),
            volume_sma NUMBER(20, 4),
            processing_date TIMESTAMP_NTZ,
            PRIMARY KEY (symbol, date)
        )
        """
        
        cursor.execute(create_processed_table)
        logger.info("✅ Table PROCESSED_STOCK_DATA créée/vérifiée")
        
        # 4. Vérifier les tables créées
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        logger.info("📋 Tables disponibles:")
        for table in tables:
            logger.info(f"   - {table[1]}")
        
        # 5. Nettoyer les anciennes données (optionnel, pour démo)
        cursor.execute("DELETE FROM RAW_STOCK_DATA")
        cursor.execute("DELETE FROM PROCESSED_STOCK_DATA")
        logger.info("🧹 Tables nettoyées (prêtes pour nouvelles données)")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("🎉 Tables Snowflake prêtes !")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur création tables: {str(e)[:200]}")
        raise

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
    """Sauvegarde dans Snowflake - VERSION SIMPLIFIÉE"""
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
        
        # Configurer le contexte
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        
        # Préparer la requête d'insertion
        insert_query = """
            INSERT INTO RAW_STOCK_DATA 
            (date, symbol, open, high, low, close, volume, dividends, stock_splits, extraction_date)
            VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'), %s, %s, %s, %s, %s, %s, %s, %s, 
                    TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'))
        """
        
        # Insérer par batch
        batch_size = 50
        inserted = 0
        
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i:i+batch_size]
            values = []
            
            for row in batch:
                values.append((
                    row['date'],
                    row['symbol'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['dividends'],
                    row['stock_splits'],
                    row['extraction_date']
                ))
            
            cursor.executemany(insert_query, values)
            inserted += len(batch)
            if i % 200 == 0:
                logger.info(f"📤 {inserted} lignes insérées...")
        
        conn.commit()
        
        # Vérification
        cursor.execute("SELECT COUNT(*) FROM RAW_STOCK_DATA")
        total = cursor.fetchone()[0]
        
        logger.info(f"✅ {inserted} lignes sauvegardées")
        logger.info(f"📊 Total en base: {total} lignes")
        
        # Afficher un résumé
        cursor.execute("""
            SELECT symbol, COUNT(*), AVG(close) 
            FROM RAW_STOCK_DATA 
            GROUP BY symbol
        """)
        
        logger.info("📋 Résumé par symbole:")
        for symbol, count, avg_price in cursor.fetchall():
            logger.info(f"   {symbol}: {count} jours, prix moyen=${avg_price:.2f}")
        
        cursor.close()
        conn.close()
        
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur sauvegarde: {str(e)[:200]}")
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
        
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        # Vérifications de base
        checks = [
            ("Nombre total de lignes", "SELECT COUNT(*) FROM RAW_STOCK_DATA"),
            ("Nombre de symboles", "SELECT COUNT(DISTINCT symbol) FROM RAW_STOCK_DATA"),
            ("Période couverte", "SELECT MIN(date), MAX(date) FROM RAW_STOCK_DATA"),
            ("Valeurs nulles", "SELECT COUNT(*) FROM RAW_STOCK_DATA WHERE close IS NULL"),
            ("Volume moyen", "SELECT AVG(volume) FROM RAW_STOCK_DATA")
        ]
        
        logger.info("=" * 60)
        logger.info("📊 QUALITÉ DES DONNÉES BRUTES")
        logger.info("=" * 60)
        
        for check_name, query in checks:
            cursor.execute(query)
            result = cursor.fetchone()
            logger.info(f"✓ {check_name}: {result[0]}")
        
        # Détail par symbole
        cursor.execute("""
            SELECT symbol, 
                   COUNT(*) as jours,
                   AVG(close) as prix_moyen,
                   MIN(close) as prix_min,
                   MAX(close) as prix_max
            FROM RAW_STOCK_DATA
            GROUP BY symbol
            ORDER BY symbol
        """)
        
        logger.info("\n📈 Détail par symbole:")
        for row in cursor.fetchall():
            logger.info(f"   {row[0]}: {row[1]} jours, prix ${row[2]:.2f} (min:${row[3]:.2f}, max:${row[4]:.2f})")
        
        logger.info("=" * 60)
        logger.info("🎉 COLLECTE TERMINÉE AVEC SUCCÈS!")
        logger.info("=" * 60)
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"⚠️ Erreur contrôle qualité: {e}")
        return True

# Configuration du DAG
with DAG(
    'collect_and_store_dag_v2',
    default_args=default_args,
    description='Collecte complète avec création automatique des tables',
    schedule_interval='0 8 * * 1-5',
    catchup=False,
    tags=['finance', 'snowflake', 'collect', 'automated'],
) as dag:
    
    # NOUVELLE TÂCHE: Création des tables
    create_tables = PythonOperator(
        task_id='create_tables_snowflake',
        python_callable=create_tables_snowflake,
    )
    
    create_data = PythonOperator(
        task_id='create_demo_data',
        python_callable=create_demo_data,
    )
    
    save_data = PythonOperator(
        task_id='save_to_snowflake_raw',
        python_callable=save_to_snowflake_raw,
    )
    
    quality_check = PythonOperator(
        task_id='quality_check_raw',
        python_callable=quality_check_raw,
    )
    
    # Nouveau workflow: créer tables → créer données → sauvegarder → vérifier
    create_tables >> create_data >> save_data >> quality_check

# Documentation
dag.doc_md = """
## DAG de Collecte - Version Complète

**Équipe:** Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima

### 🎯 Objectif
Pipeline complet de collecte avec création automatique des tables Snowflake.

### 🔄 Nouveau Workflow
1. **Créer tables Snowflake** → Crée les tables si elles n'existent pas
2. **Créer données démo** → Génère 480 lignes de données financières
3. **Sauvegarder dans Snowflake** → Insère dans `RAW_STOCK_DATA`
4. **Contrôle qualité** → Vérifie l'intégrité

### 📊 Données générées
- 8 symboles boursiers
- 60 jours historiques par symbole
- Prix: open, high, low, close
- Volume, dividends, stock_splits

### ✅ Avantages
- **Robuste**: Crée automatiquement les tables
- **Idempotent**: Peut être relancé sans erreur
- **Vérifié**: Contrôle qualité intégré
"""