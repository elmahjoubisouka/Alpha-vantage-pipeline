# 02_fetch_yahoo_data.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import yfinance as yf
import pandas as pd
import logging
import uuid

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JPM']
BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M')}"

def fetch_yahoo_data(**context):
    """Récupère les données Yahoo Finance pour tous les symboles"""
    all_data = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"Récupération de {symbol}...")
            
            # Télécharger les données (30 jours)
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="30d")
            
            if hist.empty:
                logger.warning(f"Aucune donnée pour {symbol}")
                continue
            
            # Préparer les données
            for date, row in hist.iterrows():
                all_data.append({
                    'symbol': symbol,
                    'date': date.strftime('%Y-%m-%d'),
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']),
                    'dividends': float(row.get('Dividends', 0)),
                    'stock_splits': float(row.get('Stock Splits', 0)),
                    'batch_id': BATCH_ID
                })
            
            logger.info(f"✅ {symbol}: {len(hist)} jours récupérés")
            
        except Exception as e:
            logger.error(f"❌ Erreur pour {symbol}: {str(e)}")
    
    # Sauvegarder dans XCom
    context['ti'].xcom_push(key='yahoo_data', value=all_data)
    context['ti'].xcom_push(key='batch_id', value=BATCH_ID)
    
    logger.info(f"📊 Total: {len(all_data)} lignes récupérées")
    return len(all_data)

def store_in_snowflake(**context):
    """Stocke les données dans Snowflake"""
    data = context['ti'].xcom_pull(key='yahoo_data', task_ids='fetch_yahoo_data')
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='fetch_yahoo_data')
    
    if not data:
        logger.error("Aucune donnée à stocker")
        return
    
    try:
        # Connexion Snowflake
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Préparer les données pour insertion
        insert_values = []
        for row in data:
            insert_values.append((
                row['symbol'],
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['dividends'],
                row['stock_splits'],
                batch_id
            ))
        
        # SQL d'insertion
        insert_sql = """
        INSERT INTO FINANCE_PROJECT.RAW_DATA.YAHOO_STOCKS 
        (symbol, date, open, high, low, close, volume, dividends, stock_splits, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Exécution
        cursor.executemany(insert_sql, insert_values)
        conn.commit()
        
        # Vérification
        count_query = """
        SELECT COUNT(*) 
        FROM FINANCE_PROJECT.RAW_DATA.YAHOO_STOCKS 
        WHERE batch_id = %s
        """
        cursor.execute(count_query, (batch_id,))
        count = cursor.fetchone()[0]
        
        logger.info(f"✅ {count} lignes stockées dans Snowflake (batch: {batch_id})")
        
        cursor.close()
        conn.close()
        
        return count
        
    except Exception as e:
        logger.error(f"❌ Erreur Snowflake: {str(e)}")
        raise

with DAG(
    '02_fetch_yahoo_data',
    default_args=default_args,
    schedule_interval='0 18 * * 1-5',  # 18h UTC, jours ouvrables
    catchup=False,
    tags=['yahoo', 'data_collection']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_yahoo_data',
        python_callable=fetch_yahoo_data,
    )
    
    store_task = PythonOperator(
        task_id='store_in_snowflake',
        python_callable=store_in_snowflake,
    )
    
    fetch_task >> store_task