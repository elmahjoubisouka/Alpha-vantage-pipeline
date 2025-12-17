"""
DAG: alpha_vantage_finance_pipeline
API Alpha Vantage avec clé : 3M7KBIE2THH3X52R
Base de données: FINANCE_DB (corrigé)
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
from typing import List, Dict

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - CORRIGÉE POUR FINANCE_DB
# ============================================================================
default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# VOTRE CLÉ API ALPHA VANTAGE
ALPHA_VANTAGE_API_KEY = "3M7KBIE2THH3X52R"

# VOTRE BASE DE DONNÉES (corrigée)
SNOWFLAKE_DATABASE = "FINANCE_DB"
SNOWFLAKE_SCHEMA = "RAW_DATA"

# Symboles à récupérer
PROJECT_SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JNJ']

# Batch ID unique
BATCH_ID = f"AV_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ============================================================================
# FONCTIONS DE RÉCUPÉRATION DES DONNÉES
# ============================================================================

def test_alpha_vantage_api():
    """Teste la connexion à Alpha Vantage avec votre clé"""
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": "AAPL",
            "apikey": ALPHA_VANTAGE_API_KEY
        }
        
        logger.info(f"🔍 Test connexion Alpha Vantage avec clé: {ALPHA_VANTAGE_API_KEY[:8]}...")
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if "Global Quote" in data:
            price = data['Global Quote']['05. price']
            logger.info(f"✅ Alpha Vantage CONNECTÉ! AAPL: ${price}")
            return True
        elif "Note" in data:
            logger.warning(f"⚠️ Note API: {data['Note'][:80]}")
            return True  # API fonctionne mais avec avertissement
        else:
            logger.error(f"❌ Réponse inattendue: {data}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erreur test API: {e}")
        return False

def fetch_symbol_data(symbol: str, max_days: int = 30):
    """Récupère les données d'un symbole depuis Alpha Vantage"""
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_VANTAGE_API_KEY,
            "outputsize": "compact"  # 100 derniers jours
        }
        
        logger.info(f"  📥 {symbol}...")
        
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        
        # Vérifier les erreurs
        if "Error Message" in data:
            logger.error(f"    ❌ Erreur API: {data['Error Message'][:100]}")
            return []
        
        if "Note" in data:
            logger.warning(f"    ⚠️ Limite API: {data['Note'][:80]}")
            return "RATE_LIMIT"  # Signal spécial pour rate limit
        
        if "Time Series (Daily)" not in data:
            logger.error(f"    ❌ Format invalide pour {symbol}")
            return []
        
        # Extraire les données
        time_series = data["Time Series (Daily)"]
        symbol_data = []
        days_collected = 0
        
        for date_str, values in sorted(time_series.items(), reverse=True):
            if days_collected >= max_days:
                break
            
            symbol_data.append({
                'symbol': symbol,
                'date': date_str,
                'open': float(values["1. open"]),
                'high': float(values["2. high"]),
                'low': float(values["3. low"]),
                'close': float(values["4. close"]),
                'volume': int(values["5. volume"]),
                'dividends': 0.0,
                'stock_splits': 0.0
            })
            days_collected += 1
        
        logger.info(f"    ✅ {symbol}: {days_collected} jours récupérés")
        return symbol_data
        
    except Exception as e:
        logger.error(f"    ❌ Exception pour {symbol}: {str(e)[:100]}")
        return []

def fetch_all_alpha_vantage_data():
    """Récupère les données pour tous les symboles avec gestion des limites"""
    logger.info("🚀 DÉBUT RÉCUPÉRATION ALPHA VANTAGE")
    logger.info(f"Clé API: {ALPHA_VANTAGE_API_KEY[:8]}...")
    logger.info(f"Symboles: {len(PROJECT_SYMBOLS)}")
    
    all_data = []
    rate_limit_hit = False
    
    for i, symbol in enumerate(PROJECT_SYMBOLS):
        try:
            # Récupérer les données du symbole
            symbol_data = fetch_symbol_data(symbol, max_days=30)
            
            if symbol_data == "RATE_LIMIT":
                rate_limit_hit = True
                logger.warning(f"⏸️ Limite API atteinte après {i} symboles")
                break
            
            if symbol_data:
                all_data.extend(symbol_data)
            
            # Pause IMPORTANTE entre les appels (5 appels/minute max)
            if i < len(PROJECT_SYMBOLS) - 1 and not rate_limit_hit:
                wait_time = 13  # 13 secondes entre chaque appel
                logger.info(f"⏳ Pause {wait_time}s (respect limites API)...")
                time.sleep(wait_time)
                
        except Exception as e:
            logger.error(f"Erreur globale pour {symbol}: {e}")
            continue
    
    # Ajouter les métadonnées
    for data_point in all_data:
        data_point.update({
            'batch_id': BATCH_ID,
            'data_source': 'ALPHA_VANTAGE_REAL',
            'api_key_used': ALPHA_VANTAGE_API_KEY[:8] + '...'
        })
    
    logger.info(f"📊 RÉCUPÉRATION TERMINÉE: {len(all_data)} lignes")
    
    if rate_limit_hit and len(all_data) == 0:
        logger.warning("🔄 Aucune donnée réelle, création de données de secours...")
        return create_backup_data()
    
    return all_data

def create_backup_data():
    """Crée des données réalistes en cas d'échec API"""
    logger.info("🔄 Création données de secours réalistes...")
    
    all_data = []
    end_date = datetime.now().date()
    
    # Prix réalistes basés sur des données récentes
    realistic_prices = {
        'AAPL': 195.50, 'MSFT': 370.25, 'GOOGL': 140.75, 
        'AMZN': 150.30, 'TSLA': 240.80, 'NVDA': 500.60,
        'META': 350.40, 'JNJ': 155.20
    }
    
    for symbol in PROJECT_SYMBOLS:
        base_price = realistic_prices.get(symbol, 100.00)
        
        # Générer 30 jours de données réalistes
        for i in range(30):
            date = end_date - timedelta(days=i)
            
            # Variation quotidienne réaliste
            daily_change = (hash(f"{symbol}{date}{i}") % 40 - 20) / 1000  # -2% à +2%
            close_price = round(base_price * (1 + daily_change), 2)
            
            # OHLC cohérents
            daily_volatility = close_price * 0.015  # 1.5% de volatilité
            open_price = round(close_price * (1 + ((hash(f"{symbol}{i}o") % 10 - 5) / 1000)), 2)
            high_price = round(max(open_price, close_price) + daily_volatility * 0.6, 2)
            low_price = round(min(open_price, close_price) - daily_volatility * 0.4, 2)
            
            # Volume réaliste
            base_vol = 10000000  # 10M actions
            volume = int(base_vol * (1 + (hash(f"{symbol}{i}v") % 100) / 100))
            
            all_data.append({
                'symbol': symbol,
                'date': date.strftime('%Y-%m-%d'),
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'dividends': 0.0,
                'stock_splits': 0.0,
                'batch_id': BATCH_ID,
                'data_source': 'BACKUP_SIMULATED',
                'api_key_used': 'BACKUP_DATA'
            })
    
    logger.info(f"📊 Données secours créées: {len(all_data)} lignes")
    return all_data

# ============================================================================
# TÂCHES AIRFLOW - CORRIGÉES POUR FINANCE_DB
# ============================================================================

def fetch_financial_data(**context):
    """Tâche principale de récupération des données"""
    logger.info("=" * 60)
    logger.info("🎯 PROJET FINANCE - RÉCUPÉRATION DES DONNÉES")
    logger.info(f"Équipe: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima")
    logger.info(f"Batch ID: {BATCH_ID}")
    logger.info(f"Base de données: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    logger.info("=" * 60)
    
    # Tester l'API
    api_working = test_alpha_vantage_api()
    
    if api_working:
        logger.info("✅ API Alpha Vantage fonctionnelle, récupération des données réelles...")
        all_data = fetch_all_alpha_vantage_data()
        data_source = all_data[0]['data_source'] if all_data else "NO_DATA"
    else:
        logger.warning("⚠️ API non disponible, données de secours...")
        all_data = create_backup_data()
        data_source = "BACKUP_DATA"
    
    # Statistiques
    total_rows = len(all_data)
    unique_symbols = len(set([d['symbol'] for d in all_data]))
    unique_dates = len(set([d['date'] for d in all_data]))
    
    logger.info("📈 STATISTIQUES FINALES:")
    logger.info(f"   Source: {data_source}")
    logger.info(f"   Lignes: {total_rows}")
    logger.info(f"   Symboles: {unique_symbols}")
    logger.info(f"   Jours uniques: {unique_dates}")
    if all_data:
        logger.info(f"   Période: {min([d['date'] for d in all_data])} à {max([d['date'] for d in all_data])}")
    
    # Sauvegarde XCom
    context['ti'].xcom_push(key='financial_data', value=all_data)
    context['ti'].xcom_push(key='batch_id', value=BATCH_ID)
    context['ti'].xcom_push(key='data_source', value=data_source)
    context['ti'].xcom_push(key='stats', value={
        'total_rows': total_rows,
        'unique_symbols': unique_symbols,
        'unique_dates': unique_dates,
        'data_source': data_source
    })
    
    return total_rows

def store_in_snowflake(**context):
    """Stocke les données dans Snowflake - CORRIGÉ pour FINANCE_DB"""
    data = context['ti'].xcom_pull(key='financial_data', task_ids='fetch_financial_data')
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='fetch_financial_data')
    data_source = context['ti'].xcom_pull(key='data_source', task_ids='fetch_financial_data')
    
    if not data:
        logger.error("❌ Aucune donnée à stocker")
        return 0
    
    logger.info(f"💾 STOCKAGE SNOWFLAKE")
    logger.info(f"   Database: {SNOWFLAKE_DATABASE}")
    logger.info(f"   Schema: {SNOWFLAKE_SCHEMA}")
    logger.info(f"   Lignes: {len(data)}")
    logger.info(f"   Batch: {batch_id}")
    logger.info(f"   Source: {data_source}")
    
    try:
        # Convertir en DataFrame
        df = pd.DataFrame(data)
        
        # Connexion Snowflake
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        
        # Vérifier/Créer la table dans FINANCE_DB
        cursor = conn.cursor()
        
        # 1. S'assurer qu'on utilise la bonne database
        use_db_sql = f"USE DATABASE {SNOWFLAKE_DATABASE};"
        cursor.execute(use_db_sql)
        
        # 2. Créer le schéma s'il n'existe pas
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA};"
        cursor.execute(create_schema_sql)
        
        # 3. Créer la table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ALPHA_VANTAGE_DATA (
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(10, 4),
            high DECIMAL(10, 4),
            low DECIMAL(10, 4),
            close DECIMAL(10, 4),
            volume BIGINT,
            dividends DECIMAL(10, 4),
            stock_splits DECIMAL(10, 4),
            batch_id VARCHAR(50),
            data_source VARCHAR(50),
            api_key_used VARCHAR(20),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT pk_alpha_vantage PRIMARY KEY (symbol, date, batch_id)
        )
        COMMENT = 'Données Alpha Vantage - Projet Équipe Finance'
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info(f"✅ Table créée dans {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        
        # Insertion optimisée
        from snowflake.connector.pandas_tools import write_pandas
        
        logger.info("📤 Insertion des données...")
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='ALPHA_VANTAGE_DATA',
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
            chunk_size=500,
            quote_identifiers=False
        )
        
        if success:
            # Log détaillé
            stats_sql = f"""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT symbol) as symbols,
                MIN(date) as start_date,
                MAX(date) as end_date,
                AVG(close) as avg_price
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ALPHA_VANTAGE_DATA 
            WHERE batch_id = '{batch_id}'
            """
            
            cursor.execute(stats_sql)
            stats = cursor.fetchone()
            
            logger.info(f"""
            🎉 STOCKAGE RÉUSSI:
            ├─ Database: {SNOWFLAKE_DATABASE}
            ├─ Schema: {SNOWFLAKE_SCHEMA}
            ├─ Lignes insérées: {nrows}
            ├─ Symboles: {stats[1]}
            ├─ Période: {stats[2]} à {stats[3]}
            ├─ Prix moyen: ${stats[4]:.2f}
            ├─ Source: {data_source}
            └─ Batch: {batch_id}
            """)
            
            # Créer/Créer la table de logs
            create_logs_sql = f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PIPELINE_LOGS (
                log_id INT AUTOINCREMENT,
                pipeline_name VARCHAR(100),
                batch_id VARCHAR(50),
                data_source VARCHAR(50),
                rows_processed INT,
                status VARCHAR(20),
                details VARCHAR(500),
                log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_logs_sql)
            
            # Ajouter aux logs
            log_sql = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PIPELINE_LOGS 
            (pipeline_name, batch_id, data_source, rows_processed, status, details)
            VALUES (
                'alpha_vantage_pipeline',
                '{batch_id}',
                '{data_source}',
                {nrows},
                'SUCCESS',
                'Symboles: {stats[1]}, Période: {stats[2]} à {stats[3]}'
            )
            """
            
            cursor.execute(log_sql)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return nrows
            
        else:
            logger.error("❌ Échec write_pandas")
            raise Exception("Échec de l'insertion")
            
    except Exception as e:
        logger.error(f"❌ Erreur Snowflake: {e}")
        
        # Fallback: insertion SQL manuelle
        try:
            logger.info("🔄 Tentative insertion SQL manuelle...")
            hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # Utiliser la bonne database
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE};")
            cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA};")
            
            inserted = 0
            for row in data:
                try:
                    insert_sql = f"""
                    INSERT INTO ALPHA_VANTAGE_DATA 
                    (symbol, date, open, high, low, close, volume, dividends, stock_splits, batch_id, data_source, api_key_used)
                    VALUES (
                        '{row['symbol']}', '{row['date']}', {row['open']}, {row['high']}, {row['low']}, 
                        {row['close']}, {row['volume']}, {row['dividends']}, {row['stock_splits']}, 
                        '{batch_id}', '{data_source}', '{row.get('api_key_used', 'N/A')}'
                    )
                    """
                    cursor.execute(insert_sql)
                    inserted += 1
                except Exception as e2:
                    logger.warning(f"Ligne {inserted+1} échouée: {e2}")
                    continue
            
            conn.commit()
            logger.info(f"✅ Insertion manuelle: {inserted}/{len(data)} lignes")
            
            cursor.close()
            conn.close()
            
            return inserted
            
        except Exception as e2:
            logger.error(f"❌ Échec complet: {e2}")
            raise

def generate_project_report(**context):
    """Génère un rapport pour le projet"""
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='fetch_financial_data')
    stats = context['ti'].xcom_pull(key='stats', task_ids='fetch_financial_data')
    
    logger.info("=" * 60)
    logger.info("📋 RAPPORT FINAL DU PROJET")
    logger.info("=" * 60)
    logger.info(f"ÉQUIPE: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima")
    logger.info(f"DATE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"BATCH ID: {batch_id}")
    logger.info(f"DATABASE: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    logger.info("-" * 60)
    
    if stats:
        logger.info(f"📊 DONNÉES RÉCUPÉRÉES:")
        logger.info(f"   Source: {stats['data_source']}")
        logger.info(f"   Total lignes: {stats['total_rows']}")
        logger.info(f"   Symboles uniques: {stats['unique_symbols']}")
        logger.info(f"   Jours de données: {stats['unique_dates']}")
    
    logger.info("🔧 CONFIGURATION:")
    logger.info(f"   API Alpha Vantage: ✅ Activée")
    logger.info(f"   Clé API: {ALPHA_VANTAGE_API_KEY[:8]}...")
    logger.info(f"   Symboles configurés: {len(PROJECT_SYMBOLS)}")
    logger.info(f"   Database Snowflake: {SNOWFLAKE_DATABASE}")
    logger.info(f"   Schema Snowflake: {SNOWFLAKE_SCHEMA}")
    
    logger.info("🎯 PROCHAINES ÉTAPES:")
    logger.info("   1. Vérifier les données dans Snowflake")
    logger.info("   2. Calculer les indicateurs techniques (RSI, MACD, etc.)")
    logger.info("   3. Créer les visualisations Streamlit")
    logger.info("   4. Préparer la comparaison Oozie vs Airflow")
    
    logger.info("=" * 60)
    
    # Sauvegarder le rapport
    report = {
        'team': ['Assia Boujnah', 'Soukaina El Mahjoubi', 'Khalil Fatima'],
        'project': 'Pipeline financier avec Alpha Vantage',
        'database': SNOWFLAKE_DATABASE,
        'schema': SNOWFLAKE_SCHEMA,
        'batch_id': batch_id,
        'execution_time': datetime.now().isoformat(),
        'stats': stats,
        'configuration': {
            'api_used': 'Alpha Vantage',
            'symbols_count': len(PROJECT_SYMBOLS),
            'storage': 'Snowflake',
            'next_steps': ['Technical Indicators', 'Streamlit Dashboard', 'Oozie Comparison']
        }
    }
    
    context['ti'].xcom_push(key='project_report', value=report)
    
    return report

# ============================================================================
# DAG PRINCIPAL - CORRIGÉ POUR FINANCE_DB
# ============================================================================

with DAG(
    'alpha_vantage_finance_pipeline',
    default_args=default_args,
    description=f'Pipeline financier avec API Alpha Vantage - Database: {SNOWFLAKE_DATABASE}',
    schedule_interval='0 20 * * 1-5',  # 20h UTC, jours ouvrables
    catchup=False,
    tags=['alpha_vantage', 'finance', 'project', 'equipe', SNOWFLAKE_DATABASE.lower()]
) as dag:

    # Tâche 1: Récupération des données
    fetch_task = PythonOperator(
        task_id='fetch_financial_data',
        python_callable=fetch_financial_data,
    )
    
    # Tâche 2: Stockage Snowflake
    store_task = PythonOperator(
        task_id='store_in_snowflake',
        python_callable=store_in_snowflake,
    )
    
    # Tâche 3: Rapport projet
    report_task = PythonOperator(
        task_id='generate_project_report',
        python_callable=generate_project_report,
    )
    
    # Orchestration
    fetch_task >> store_task >> report_task