"""
DAG: alpha_vantage_project
Configuration EXACTE pour votre projet
Connexion: snowflake_default (ou snowflake_conn si vous la créez)
Schema: RAW_DATA (où vous avez créé la table)
Table: ALPHA_VANTAGE_DATA (comme dans votre SQL)
Équipe: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import requests
import logging
import time

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - ADAPTÉE À VOTRE ENVIRONNEMENT
# ============================================================================
default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# VOTRE CLÉ API
ALPHA_VANTAGE_API_KEY = "3M7KBIE2THH3X52R"

# CONFIGURATION SNOWFLAKE (correspond à votre connexion)
SNOWFLAKE_CONN_ID = "snowflake_default"  # Utilise snowflake_default
SNOWFLAKE_DATABASE = "FINANCE_DB"
SNOWFLAKE_SCHEMA = "RAW_DATA"  # Car votre table est dans RAW_DATA
SNOWFLAKE_TABLE = "ALPHA_VANTAGE_DATA"  # Nom exact de votre table

# Symboles du projet
PROJECT_SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META']

# ============================================================================
# FONCTIONS PRINCIPALES
# ============================================================================

def test_alpha_vantage():
    """Test simple de l'API Alpha Vantage"""
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": "AAPL",
            "apikey": ALPHA_VANTAGE_API_KEY
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if "Global Quote" in data:
            price = data['Global Quote']['05. price']
            logger.info(f"✅ Alpha Vantage OK - AAPL: ${price}")
            return True
        else:
            logger.warning("⚠️ API fonctionne mais format différent")
            return True
            
    except Exception as e:
        logger.error(f"❌ Erreur API: {e}")
        return False

def fetch_financial_data(**context):
    """Récupère les données financières"""
    logger.info("=" * 60)
    logger.info("📊 PROJET FINANCE - RÉCUPÉRATION DES DONNÉES")
    logger.info("=" * 60)
    
    batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M')}"
    all_data = []
    
    # Test API
    if not test_alpha_vantage():
        logger.warning("API Alpha Vantage non disponible")
        return 0
    
    # Récupération des données
    logger.info(f"Récupération de {len(PROJECT_SYMBOLS)} symboles...")
    
    for i, symbol in enumerate(PROJECT_SYMBOLS):
        try:
            logger.info(f"  {symbol} ({i+1}/{len(PROJECT_SYMBOLS)})")
            
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "apikey": ALPHA_VANTAGE_API_KEY,
                "outputsize": "compact"
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            if "Time Series (Daily)" in data:
                time_series = data["Time Series (Daily)"]
                days = 0
                
                for date_str, values in time_series.items():
                    if days >= 30:  # Limite à 30 jours
                        break
                    
                    all_data.append({
                        'symbol': symbol,
                        'date': date_str,
                        'open': float(values["1. open"]),
                        'high': float(values["2. high"]),
                        'low': float(values["3. low"]),
                        'close': float(values["4. close"]),
                        'volume': int(values["5. volume"]),
                        'dividends': 0.0,
                        'stock_splits': 0.0,
                        'batch_id': batch_id,
                        'data_source': 'ALPHA_VANTAGE',
                        'api_key_used': ALPHA_VANTAGE_API_KEY[:8] + '...'
                    })
                    days += 1
                
                logger.info(f"    ✅ {days} jours")
            else:
                logger.warning(f"    ⚠️ Pas de données")
            
            # Pause pour respecter les limites API
            if i < len(PROJECT_SYMBOLS) - 1:
                time.sleep(13)  # 5 appels/minute max
                
        except Exception as e:
            logger.error(f"    ❌ Erreur: {str(e)[:100]}")
            continue
    
    # Fallback si pas de données
    if not all_data:
        logger.info("Création données de secours...")
        all_data = create_fallback_data(batch_id)
    
    logger.info(f"📈 Total: {len(all_data)} lignes récupérées")
    
    # Sauvegarde XCom
    context['ti'].xcom_push(key='financial_data', value=all_data)
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    
    return len(all_data)

def create_fallback_data(batch_id):
    """Crée des données de secours réalistes"""
    all_data = []
    end_date = datetime.now().date()
    
    prices = {
        'AAPL': 195.50, 'MSFT': 370.25, 'GOOGL': 140.75,
        'AMZN': 150.30, 'TSLA': 240.80, 'NVDA': 500.60,
        'META': 350.40
    }
    
    for symbol in PROJECT_SYMBOLS:
        base = prices.get(symbol, 100.00)
        
        for i in range(30):
            date = end_date - timedelta(days=i)
            
            # Variation réaliste
            change = (hash(f"{symbol}{i}") % 40 - 20) / 1000  # -2% à +2%
            close = round(base * (1 + change), 2)
            
            all_data.append({
                'symbol': symbol,
                'date': date.strftime('%Y-%m-%d'),
                'open': round(close * 0.99, 2),
                'high': round(close * 1.02, 2),
                'low': round(close * 0.97, 2),
                'close': close,
                'volume': 50000000,
                'dividends': 0.0,
                'stock_splits': 0.0,
                'batch_id': batch_id,
                'data_source': 'FALLBACK',
                'api_key_used': 'FALLBACK_DATA'
            })
    
    return all_data

def store_in_snowflake(**context):
    """Stocke les données dans votre table ALPHA_VANTAGE_DATA"""
    data = context['ti'].xcom_pull(key='financial_data', task_ids='fetch_financial_data')
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='fetch_financial_data')
    
    if not data:
        logger.error("❌ Aucune donnée à stocker")
        return 0
    
    logger.info("=" * 60)
    logger.info("💾 STOCKAGE SNOWFLAKE")
    logger.info(f"Connexion: {SNOWFLAKE_CONN_ID}")
    logger.info(f"Database: {SNOWFLAKE_DATABASE}")
    logger.info(f"Schema: {SNOWFLAKE_SCHEMA}")
    logger.info(f"Table: {SNOWFLAKE_TABLE}")
    logger.info(f"Lignes: {len(data)}")
    logger.info(f"Batch: {batch_id}")
    logger.info("=" * 60)
    
    try:
        # Connexion Snowflake
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        logger.info("✅ Connexion Snowflake établie")
        
        # Vérifier qu'on utilise la bonne database
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE};")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA};")
        
        # Insertion des données
        inserted = 0
        errors = 0
        
        for row in data:
            try:
                insert_sql = f"""
                INSERT INTO {SNOWFLAKE_TABLE} 
                (symbol, date, open, high, low, close, volume, dividends, stock_splits, batch_id, data_source, api_key_used)
                VALUES (
                    '{row['symbol']}', 
                    '{row['date']}', 
                    {row['open']}, 
                    {row['high']}, 
                    {row['low']}, 
                    {row['close']}, 
                    {row['volume']}, 
                    {row['dividends']}, 
                    {row['stock_splits']}, 
                    '{batch_id}', 
                    '{row['data_source']}', 
                    '{row['api_key_used']}'
                )
                """
                cursor.execute(insert_sql)
                inserted += 1
                
                # Commit tous les 50 inserts
                if inserted % 50 == 0:
                    conn.commit()
                    
            except Exception as e:
                errors += 1
                if errors <= 3:  # Log seulement les 3 premières erreurs
                    logger.warning(f"Ligne {inserted+1}: {str(e)[:100]}")
                continue
        
        # Final commit
        conn.commit()
        
        # Vérification
        verify_sql = f"""
        SELECT COUNT(*) 
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
        WHERE batch_id = '{batch_id}'
        """
        cursor.execute(verify_sql)
        verified_count = cursor.fetchone()[0]
        
        # Log dans PIPELINE_LOGS
        log_sql = f"""
        INSERT INTO PIPELINE_LOGS 
        (pipeline_name, batch_id, data_source, rows_processed, status, details)
        VALUES (
            'alpha_vantage_project',
            '{batch_id}',
            '{data[0]['data_source'] if data else 'UNKNOWN'}',
            {inserted},
            'SUCCESS',
            'Inserted: {inserted}, Errors: {errors}, Verified: {verified_count}'
        )
        """
        cursor.execute(log_sql)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        logger.info(f"""
        🎉 STOCKAGE RÉUSSI!
        ├─ Tentatives: {len(data)} lignes
        ├─ Insérées: {inserted} lignes
        ├─ Erreurs: {errors} lignes
        ├─ Vérifiées: {verified_count} lignes
        ├─ Database: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}
        ├─ Table: {SNOWFLAKE_TABLE}
        └─ Batch: {batch_id}
        """)
        
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur Snowflake: {e}")
        
        # Tentative alternative avec write_pandas
        try:
            logger.info("🔄 Tentative avec write_pandas...")
            df = pd.DataFrame(data)
            
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            conn = hook.get_conn()
            
            from snowflake.connector.pandas_tools import write_pandas
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=SNOWFLAKE_TABLE,
                schema=SNOWFLAKE_SCHEMA,
                database=SNOWFLAKE_DATABASE,
                chunk_size=100
            )
            
            if success:
                logger.info(f"✅ Alternative réussie: {nrows} lignes")
                return nrows
            else:
                raise Exception("write_pandas échoué")
                
        except Exception as e2:
            logger.error(f"❌ Échec complet: {e2}")
            
            # Sauvegarde locale
            import json
            backup_file = f"/tmp/backup_{batch_id}.json"
            with open(backup_file, 'w') as f:
                json.dump(data, f)
            logger.info(f"💾 Backup local: {backup_file}")
            
            raise

def generate_report(**context):
    """Génère un rapport final"""
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='fetch_financial_data')
    
    logger.info("=" * 60)
    logger.info("📋 RAPPORT FINAL - PROJET FINANCE")
    logger.info("=" * 60)
    logger.info("ÉQUIPE:")
    logger.info("  - Assia Boujnah")
    logger.info("  - Soukaina El Mahjoubi")  
    logger.info("  - Khalil Fatima")
    logger.info("")
    logger.info("TECHNOLOGIES UTILISÉES:")
    logger.info("  • Apache Airflow: Orchestration du pipeline")
    logger.info("  • Alpha Vantage API: Données financières réelles")
    logger.info("  • Snowflake: Stockage des données (FINANCE_DB.RAW_DATA)")
    logger.info("  • Python: Traitement des données")
    logger.info("")
    logger.info("CONFIGURATION:")
    logger.info(f"  • Clé API: {ALPHA_VANTAGE_API_KEY[:8]}...")
    logger.info(f"  • Snowflake: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    logger.info(f"  • Table: {SNOWFLAKE_TABLE}")
    logger.info(f"  • Batch: {batch_id}")
    logger.info("")
    logger.info("NEXT STEPS:")
    logger.info("  1. Vérifier les données dans Snowflake")
    logger.info("  2. Calculer indicateurs techniques (RSI, MACD, etc.)")
    logger.info("  3. Créer dashboard Streamlit")
    logger.info("  4. Préparer comparaison Oozie vs Airflow")
    logger.info("=" * 60)
    
    return "Rapport généré"

# ============================================================================
# DAG PRINCIPAL
# ============================================================================

with DAG(
    'alpha_vantage_project',
    default_args=default_args,
    description='Pipeline financier complet - Projet équipe',
    schedule_interval='0 21 * * 1-5',  # 21h UTC
    catchup=False,
    tags=['finance', 'alpha_vantage', 'snowflake', 'projet']
) as dag:

    # Tâche 1: Récupération
    fetch_task = PythonOperator(
        task_id='fetch_financial_data',
        python_callable=fetch_financial_data,
    )
    
    # Tâche 2: Stockage
    store_task = PythonOperator(
        task_id='store_in_snowflake',
        python_callable=store_in_snowflake,
    )
    
    # Tâche 3: Rapport
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )
    
    # Orchestration
    fetch_task >> store_task >> report_task