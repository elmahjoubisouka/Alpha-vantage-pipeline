"""
DAG: yahoo_finance_pipeline - DONNÉES RÉELLES DE YAHOO FINANCE
Équipe: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any
import uuid

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['assia.soukaina.khalil@finance.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Liste des symboles à tracker
SYMBOLS = [
    'AAPL',    # Apple
    'MSFT',    # Microsoft
    'GOOGL',   # Google (Alphabet)
    'AMZN',    # Amazon
    'TSLA',    # Tesla
    'NVDA',    # NVIDIA
    'META',    # Meta (Facebook)
    'NFLX',    # Netflix
    'JPM',     # JPMorgan Chase
    'JNJ',     # Johnson & Johnson
    'V',       # Visa
    'PG',      # Procter & Gamble
    'MA',      # Mastercard
    'DIS',     # Disney
    'BAC',     # Bank of America
    'ADBE',    # Adobe
]

def create_yahoo_tables(**context):
    """Crée les tables Yahoo Finance dans Snowflake si elles n'existent pas"""
    logger.info("🏗️  Vérification/création des tables Yahoo Finance...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Configuration du contexte
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        # 1. Table pour données brutes Yahoo
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS YAHOO_RAW_DATA (
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open NUMBER(20, 4),
                high NUMBER(20, 4),
                low NUMBER(20, 4),
                close NUMBER(20, 4),
                volume NUMBER(38, 0),
                dividends NUMBER(20, 4) DEFAULT 0,
                stock_splits NUMBER(20, 4) DEFAULT 0,
                data_source VARCHAR(50) DEFAULT 'YAHOO_FINANCE',
                extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                batch_id VARCHAR(50),
                PRIMARY KEY (symbol, date)
            )
        """)
        
        # 2. Table pour données traitées
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS YAHOO_PROCESSED_DATA (
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open NUMBER(20, 4),
                high NUMBER(20, 4),
                low NUMBER(20, 4),
                close NUMBER(20, 4),
                volume NUMBER(38, 0),
                rsi_14 NUMBER(10, 4),
                macd NUMBER(20, 6),
                macd_signal NUMBER(20, 6),
                macd_histogram NUMBER(20, 6),
                sma_20 NUMBER(20, 4),
                sma_50 NUMBER(20, 4),
                ema_12 NUMBER(20, 4),
                ema_26 NUMBER(20, 4),
                bb_upper NUMBER(20, 4),
                bb_middle NUMBER(20, 4),
                bb_lower NUMBER(20, 4),
                adx_14 NUMBER(10, 4),
                atr_14 NUMBER(20, 4),
                obv NUMBER(38, 0),
                processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                source_batch_id VARCHAR(50),
                data_quality VARCHAR(20),
                PRIMARY KEY (symbol, date)
            )
        """)
        
        # 3. Table de métadonnées
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ETL_METADATA (
                batch_id VARCHAR(50) PRIMARY KEY,
                extraction_date DATE,
                symbols_extracted VARCHAR(1000),
                total_records NUMBER(10, 0),
                start_time TIMESTAMP_NTZ,
                end_time TIMESTAMP_NTZ,
                status VARCHAR(20),
                error_message VARCHAR(1000),
                duration_seconds NUMBER(10, 0),
                data_source VARCHAR(50)
            )
        """)
        
        # Vérification
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        logger.info("📋 Tables disponibles:")
        for table in tables:
            logger.info(f"   - {table[1]}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("✅ Tables Yahoo Finance prêtes !")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur création tables: {str(e)[:200]}")
        raise

def fetch_yahoo_finance_data(**context):
    """Récupère les données RÉELLES de Yahoo Finance API"""
    logger.info("📡 Récupération des données Yahoo Finance en cours...")
    
    batch_id = f"YAHOO_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    start_time = datetime.now()
    all_data = []
    successful_symbols = []
    failed_symbols = []
    
    # Configuration yfinance
    period = "3mo"  # 3 mois de données historiques
    interval = "1d"  # Données quotidiennes
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"🔍 Récupération de {symbol}...")
            
            # Récupérer les données depuis Yahoo Finance
            ticker = yf.Ticker(symbol)
            
            # Données historiques
            hist = ticker.history(period=period, interval=interval)
            
            if hist.empty:
                logger.warning(f"⚠️ Aucune donnée pour {symbol}")
                failed_symbols.append(symbol)
                continue
            
            # Informations supplémentaires
            info = ticker.info
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 0))
            
            logger.info(f"✅ {symbol}: {len(hist)} jours - Prix actuel: ${current_price:.2f}")
            
            # Convertir en format compatible Snowflake
            for index, row in hist.iterrows():
                all_data.append({
                    'symbol': symbol,
                    'date': index.strftime('%Y-%m-%d'),
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']),
                    'dividends': float(row.get('Dividends', 0)),
                    'stock_splits': float(row.get('Stock Splits', 0)),
                    'batch_id': batch_id
                })
            
            successful_symbols.append(symbol)
            
            # Petite pause pour éviter le rate limiting
            import time
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"❌ Erreur pour {symbol}: {str(e)[:100]}")
            failed_symbols.append(symbol)
            continue
    
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    # Préparer les métadonnées
    metadata = {
        'batch_id': batch_id,
        'extraction_date': datetime.now().date().isoformat(),
        'symbols_extracted': ','.join(successful_symbols),
        'total_records': len(all_data),
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'SUCCESS' if successful_symbols else 'FAILED',
        'error_message': f"Failed: {','.join(failed_symbols)}" if failed_symbols else None,
        'duration_seconds': duration,
        'data_source': 'YAHOO_FINANCE_API'
    }
    
    logger.info(f"📊 RÉSUMÉ YAHOO FINANCE:")
    logger.info(f"   Symboles réussis: {len(successful_symbols)}/{len(SYMBOLS)}")
    logger.info(f"   Lignes récupérées: {len(all_data)}")
    logger.info(f"   Durée: {duration} secondes")
    logger.info(f"   Batch ID: {batch_id}")
    
    if failed_symbols:
        logger.warning(f"⚠️ Symboles échoués: {failed_symbols}")
    
    # Stocker dans XCom
    context['ti'].xcom_push(key='yahoo_raw_data', value=all_data)
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    context['ti'].xcom_push(key='metadata', value=metadata)
    
    return all_data

def save_yahoo_to_snowflake(**context):
    """Sauvegarde les données Yahoo Finance dans Snowflake"""
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_yahoo_finance_data', key='yahoo_raw_data')
    batch_id = ti.xcom_pull(task_ids='fetch_yahoo_finance_data', key='batch_id')
    
    if not raw_data:
        logger.error("❌ Aucune donnée Yahoo Finance à sauvegarder")
        return 0
    
    logger.info(f"💾 Sauvegarde de {len(raw_data)} lignes Yahoo Finance...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Configuration
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        # 1. Sauvegarder les données brutes
        insert_raw_query = """
            INSERT INTO YAHOO_RAW_DATA 
            (symbol, date, open, high, low, close, volume, dividends, stock_splits, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) 
            DO UPDATE SET 
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                dividends = EXCLUDED.dividends,
                stock_splits = EXCLUDED.stock_splits,
                extraction_timestamp = CURRENT_TIMESTAMP(),
                batch_id = EXCLUDED.batch_id
        """
        
        batch_size = 100
        inserted = 0
        
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i:i+batch_size]
            values = []
            
            for row in batch:
                values.append((
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
            
            cursor.executemany(insert_raw_query, values)
            inserted += len(batch)
            
            if i % 500 == 0:
                logger.info(f"📤 {inserted} lignes insérées...")
        
        # 2. Sauvegarder les métadonnées
        metadata = ti.xcom_pull(task_ids='fetch_yahoo_finance_data', key='metadata')
        
        if metadata:
            insert_meta_query = """
                INSERT INTO ETL_METADATA 
                (batch_id, extraction_date, symbols_extracted, total_records, 
                 start_time, end_time, status, error_message, duration_seconds, data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_meta_query, (
                metadata['batch_id'],
                metadata['extraction_date'],
                metadata['symbols_extracted'],
                metadata['total_records'],
                metadata['start_time'],
                metadata['end_time'],
                metadata['status'],
                metadata['error_message'],
                metadata['duration_seconds'],
                metadata['data_source']
            ))
        
        conn.commit()
        
        # Vérifications
        cursor.execute("SELECT COUNT(*) FROM YAHOO_RAW_DATA WHERE batch_id = %s", (batch_id,))
        count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT symbol, COUNT(*) as days, 
                   AVG(close) as avg_price, 
                   MIN(date) as first_date,
                   MAX(date) as last_date
            FROM YAHOO_RAW_DATA 
            WHERE batch_id = %s
            GROUP BY symbol
            ORDER BY symbol
        """, (batch_id,))
        
        logger.info("📊 STATISTIQUES YAHOO FINANCE:")
        logger.info(f"   Lignes insérées: {inserted}")
        logger.info(f"   Vérification batch: {count} lignes")
        
        for symbol, days, avg_price, first_date, last_date in cursor.fetchall():
            logger.info(f"   {symbol}: {days} jours (${avg_price:.2f}) [{first_date} to {last_date}]")
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Données Yahoo Finance sauvegardées! Batch: {batch_id}")
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur sauvegarde Yahoo: {str(e)[:200]}")
        raise

def calculate_technical_indicators(**context):
    """Calcule les indicateurs techniques sur les données Yahoo"""
    logger.info("📈 Calcul des indicateurs techniques...")
    
    try:
        ti = context['ti']
        batch_id = ti.xcom_pull(task_ids='fetch_yahoo_finance_data', key='batch_id')
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        # Récupérer les données pour calcul
        cursor.execute("""
            SELECT symbol, date, open, high, low, close, volume
            FROM YAHOO_RAW_DATA
            WHERE batch_id = %s
            ORDER BY symbol, date
        """, (batch_id,))
        
        data = cursor.fetchall()
        
        if not data:
            logger.warning("⚠️ Aucune donnée pour calculer les indicateurs")
            return 0
        
        # Convertir en DataFrame pandas pour calculs
        df = pd.DataFrame(data, columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
        
        processed_data = []
        
        for symbol in df['symbol'].unique():
            symbol_df = df[df['symbol'] == symbol].copy()
            symbol_df = symbol_df.sort_values('date')
            
            if len(symbol_df) < 50:  # Besoin de suffisamment de données
                continue
            
            # Convertir en numérique
            for col in ['open', 'high', 'low', 'close', 'volume']:
                symbol_df[col] = pd.to_numeric(symbol_df[col], errors='coerce')
            
            # Moyennes mobiles simples
            symbol_df['sma_20'] = symbol_df['close'].rolling(window=20).mean()
            symbol_df['sma_50'] = symbol_df['close'].rolling(window=50).mean()
            
            # Moyennes mobiles exponentielles (pour MACD)
            symbol_df['ema_12'] = symbol_df['close'].ewm(span=12, adjust=False).mean()
            symbol_df['ema_26'] = symbol_df['close'].ewm(span=26, adjust=False).mean()
            symbol_df['macd'] = symbol_df['ema_12'] - symbol_df['ema_26']
            symbol_df['macd_signal'] = symbol_df['macd'].ewm(span=9, adjust=False).mean()
            symbol_df['macd_histogram'] = symbol_df['macd'] - symbol_df['macd_signal']
            
            # RSI
            delta = symbol_df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            symbol_df['rsi_14'] = 100 - (100 / (1 + rs))
            
            # Bollinger Bands
            symbol_df['bb_middle'] = symbol_df['close'].rolling(window=20).mean()
            bb_std = symbol_df['close'].rolling(window=20).std()
            symbol_df['bb_upper'] = symbol_df['bb_middle'] + (bb_std * 2)
            symbol_df['bb_lower'] = symbol_df['bb_middle'] - (bb_std * 2)
            
            # On Balance Volume (OBV)
            symbol_df['obv'] = 0
            for i in range(1, len(symbol_df)):
                if symbol_df['close'].iloc[i] > symbol_df['close'].iloc[i-1]:
                    symbol_df.loc[symbol_df.index[i], 'obv'] = symbol_df['obv'].iloc[i-1] + symbol_df['volume'].iloc[i]
                elif symbol_df['close'].iloc[i] < symbol_df['close'].iloc[i-1]:
                    symbol_df.loc[symbol_df.index[i], 'obv'] = symbol_df['obv'].iloc[i-1] - symbol_df['volume'].iloc[i]
                else:
                    symbol_df.loc[symbol_df.index[i], 'obv'] = symbol_df['obv'].iloc[i-1]
            
            # Ajouter aux données traitées
            for _, row in symbol_df.iterrows():
                if pd.notna(row['rsi_14']) and pd.notna(row['macd']):
                    processed_data.append({
                        'symbol': symbol,
                        'date': row['date'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume'],
                        'rsi_14': row['rsi_14'],
                        'macd': row['macd'],
                        'macd_signal': row['macd_signal'],
                        'macd_histogram': row['macd_histogram'],
                        'sma_20': row['sma_20'],
                        'sma_50': row['sma_50'],
                        'ema_12': row['ema_12'],
                        'ema_26': row['ema_26'],
                        'bb_upper': row['bb_upper'],
                        'bb_middle': row['bb_middle'],
                        'bb_lower': row['bb_lower'],
                        'obv': row['obv'],
                        'source_batch_id': batch_id,
                        'data_quality': 'GOOD' if pd.notna(row['rsi_14']) else 'WARNING'
                    })
        
        # Sauvegarder les données traitées
        insert_processed_query = """
            INSERT INTO YAHOO_PROCESSED_DATA 
            (symbol, date, open, high, low, close, volume,
             rsi_14, macd, macd_signal, macd_histogram,
             sma_20, sma_50, ema_12, ema_26,
             bb_upper, bb_middle, bb_lower, obv,
             source_batch_id, data_quality)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) 
            DO UPDATE SET 
                rsi_14 = EXCLUDED.rsi_14,
                macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                macd_histogram = EXCLUDED.macd_histogram,
                sma_20 = EXCLUDED.sma_20,
                sma_50 = EXCLUDED.sma_50,
                ema_12 = EXCLUDED.ema_12,
                ema_26 = EXCLUDED.ema_26,
                bb_upper = EXCLUDED.bb_upper,
                bb_middle = EXCLUDED.bb_middle,
                bb_lower = EXCLUDED.bb_lower,
                obv = EXCLUDED.obv,
                processing_timestamp = CURRENT_TIMESTAMP(),
                source_batch_id = EXCLUDED.source_batch_id,
                data_quality = EXCLUDED.data_quality
        """
        
        inserted = 0
        for row in processed_data:
            cursor.execute(insert_processed_query, (
                row['symbol'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'],
                row['rsi_14'], row['macd'], row['macd_signal'], row['macd_histogram'],
                row['sma_20'], row['sma_50'], row['ema_12'], row['ema_26'],
                row['bb_upper'], row['bb_middle'], row['bb_lower'], row['obv'],
                row['source_batch_id'], row['data_quality']
            ))
            inserted += 1
        
        conn.commit()
        
        logger.info(f"✅ {inserted} lignes traitées avec indicateurs techniques")
        
        # Statistiques
        cursor.execute("""
            SELECT symbol, 
                   AVG(rsi_14) as avg_rsi,
                   AVG(macd) as avg_macd,
                   COUNT(*) as processed_days
            FROM YAHOO_PROCESSED_DATA
            WHERE source_batch_id = %s
            GROUP BY symbol
        """, (batch_id,))
        
        logger.info("📈 INDICATEURS CALCULÉS:")
        for symbol, avg_rsi, avg_macd, days in cursor.fetchall():
            logger.info(f"   {symbol}: RSI={avg_rsi:.1f}, MACD={avg_macd:.4f} ({days} jours)")
        
        cursor.close()
        conn.close()
        
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur calcul indicateurs: {str(e)[:200]}")
        raise

def generate_yahoo_report(**context):
    """Génère un rapport complet des données Yahoo Finance"""
    logger.info("📋 Génération du rapport Yahoo Finance...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("USE SCHEMA STOCK_DATA")
        
        # Rapport complet
        queries = [
            ("Données brutes totales", "SELECT COUNT(*) FROM YAHOO_RAW_DATA"),
            ("Données traitées totales", "SELECT COUNT(*) FROM YAHOO_PROCESSED_DATA"),
            ("Dernier batch", "SELECT MAX(batch_id) FROM YAHOO_RAW_DATA"),
            ("Symboles uniques", "SELECT COUNT(DISTINCT symbol) FROM YAHOO_RAW_DATA"),
            ("Période couverte", "SELECT MIN(date), MAX(date) FROM YAHOO_RAW_DATA"),
            ("Volume moyen journalier", "SELECT AVG(volume) FROM YAHOO_RAW_DATA"),
            ("Prix moyen de clôture", "SELECT AVG(close) FROM YAHOO_RAW_DATA"),
            ("RSI moyen", "SELECT AVG(rsi_14) FROM YAHOO_PROCESSED_DATA"),
            ("Top 5 par capitalisation", """
                SELECT symbol, AVG(close * volume) as avg_market_cap
                FROM YAHOO_RAW_DATA 
                GROUP BY symbol 
                ORDER BY avg_market_cap DESC 
                LIMIT 5
            """),
        ]
        
        logger.info("=" * 70)
        logger.info("📊 RAPPORT COMPLET YAHOO FINANCE")
        logger.info("=" * 70)
        
        for title, query in queries:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                logger.info(f"{title}: {result[0]}")
            except:
                logger.info(f"{title}: N/A")
        
        # Détail par symbole
        cursor.execute("""
            SELECT r.symbol, 
                   COUNT(r.date) as days,
                   AVG(r.close) as avg_price,
                   AVG(p.rsi_14) as avg_rsi,
                   MAX(r.date) as latest_date,
                   r.close as latest_price
            FROM YAHOO_RAW_DATA r
            LEFT JOIN YAHOO_PROCESSED_DATA p ON r.symbol = p.symbol AND r.date = p.date
            WHERE r.date = (SELECT MAX(date) FROM YAHOO_RAW_DATA WHERE symbol = r.symbol)
            GROUP BY r.symbol, r.close
            ORDER BY avg_price DESC
        """)
        
        logger.info("\n📈 DERNIÈRES DONNÉES PAR SYMBOLE:")
        logger.info("-" * 70)
        logger.info(f"{'Symbole':<8} {'Jours':<6} {'Prix Moyen':<12} {'RSI':<8} {'Dernier':<12} {'Prix':<10}")
        logger.info("-" * 70)
        
        for symbol, days, avg_price, avg_rsi, latest_date, latest_price in cursor.fetchall():
            logger.info(f"{symbol:<8} {days:<6} ${avg_price:<11.2f} {avg_rsi or 0:<7.1f} {latest_date:%Y-%m-%d} ${latest_price:<9.2f}")
        
        logger.info("=" * 70)
        logger.info("✅ PIPELINE YAHOO FINANCE TERMINÉ AVEC SUCCÈS!")
        logger.info("=" * 70)
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"⚠️ Erreur rapport: {e}")
        return True

# Configuration du DAG
with DAG(
    'yahoo_finance_pipeline',
    default_args=default_args,
    description='Pipeline complet avec données RÉELLES de Yahoo Finance API',
    schedule_interval='0 18 * * 1-5',  # 18h chaque jour ouvré
    catchup=False,
    max_active_runs=1,
    tags=['yahoo', 'finance', 'real_data', 'api', 'snowflake'],
) as dag:
    
    # Tâches
    create_tables = PythonOperator(
        task_id='create_yahoo_tables',
        python_callable=create_yahoo_tables,
    )
    
    fetch_data = PythonOperator(
        task_id='fetch_yahoo_finance_data',
        python_callable=fetch_yahoo_finance_data,
    )
    
    save_data = PythonOperator(
        task_id='save_yahoo_to_snowflake',
        python_callable=save_yahoo_to_snowflake,
    )
    
    calculate_indicators = PythonOperator(
        task_id='calculate_technical_indicators',
        python_callable=calculate_technical_indicators,
    )
    
    generate_report = PythonOperator(
        task_id='generate_yahoo_report',
        python_callable=generate_yahoo_report,
    )
    
    # Workflow
    create_tables >> fetch_data >> save_data >> calculate_indicators >> generate_report

# Documentation
dag.doc_md = """
## 📡 Pipeline Yahoo Finance - DONNÉES RÉELLES

**Équipe:** Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima

### 🎯 Objectif
Pipeline ETL complet avec données réelles de l'API Yahoo Finance.

### 🔄 Workflow
1. **Création tables** → Préparation des tables Snowflake
2. **Récupération API** → Données réelles de Yahoo Finance (16 symboles)
3. **Sauvegarde** → Stockage dans Snowflake
4. **Calcul indicateurs** → RSI, MACD, Bollinger Bands, etc.
5. **Rapport** → Analyse complète

### 📊 Données récupérées
- **16 symboles** boursiers majeurs
- **3 mois** d'historique quotidien
- **Données complètes**: Open, High, Low, Close, Volume
- **Dividendes** et **Stock Splits**

### 🛠️ Technologies
- **yfinance** : Bibliothèque Python pour Yahoo Finance
- **Snowflake** : Stockage cloud
- **Airflow** : Orchestration
- **Pandas** : Calcul des indicateurs

### ⚠️ Notes
- Nécessite une connexion internet pour l'API
- Rate limiting: ~2 requêtes/seconde
- Données en temps réel (délai de 15-20 minutes)
"""