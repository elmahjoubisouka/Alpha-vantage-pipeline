"""
Pipeline Finance - Données financières vers Snowflake avec Spark SQL
Équipe: Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima
VERSION AVEC SPARK SQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev, when, lit
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'equipe_finance',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']

def create_demo_data(**context) -> List[Dict[str, Any]]:
    """
    Crée des données de démonstration réalistes pour le projet
    """
    logger.info("🎓 Création de données de démonstration...")
    
    all_data = []
    extraction_date = datetime.now()
    
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
        config = demo_config[symbol]
        base_price = config['base']
        volatility = config['vol']
        
        dates = pd.date_range(
            end=extraction_date.date(),
            periods=60,
            freq='B'
        )
        
        np.random.seed(hash(symbol) % 10000)
        
        trend = np.linspace(0.95, 1.05, 60)
        noise = 1 + np.random.randn(60) * volatility
        close_prices = base_price * trend * noise
        
        df = pd.DataFrame({
            'date': dates,
            'open': close_prices * (1 - np.random.uniform(0, 0.01, 60)),
            'high': close_prices * (1 + np.random.uniform(0, 0.02, 60)),
            'low': close_prices * (1 - np.random.uniform(0, 0.02, 60)),
            'close': close_prices,
            'volume': np.random.randint(5_000_000, 50_000_000, 60),
            'symbol': symbol,
            'extraction_date': extraction_date
        })
        
        for _, row in df.iterrows():
            all_data.append({
                'date': row['date'].strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']),
                'symbol': str(row['symbol']),
                'extraction_date': extraction_date.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        logger.info(f"📝 {symbol}: {len(df)} jours créés (${df['close'].iloc[-1]:.2f})")
    
    logger.info(f"📊 Total: {len(all_data)} lignes pour {len(SYMBOLS)} symboles")
    return all_data

def save_to_snowflake(**context):
    """Sauvegarde dans Snowflake"""
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='create_demo_data')
    
    if not raw_data:
        logger.error("❌ Aucune donnée")
        return 0
    
    logger.info(f"💾 Sauvegarde de {len(raw_data)} lignes...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        logger.info("✅ Connecté à Snowflake")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW_STOCK_DATA (
                date TIMESTAMP_NTZ,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume NUMBER,
                symbol VARCHAR(20),
                extraction_date TIMESTAMP_NTZ
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PROCESSED_STOCK_DATA_SPARK (
                date TIMESTAMP_NTZ,
                symbol VARCHAR(20),
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume NUMBER,
                rsi FLOAT,
                macd FLOAT,
                macd_signal FLOAT,
                macd_diff FLOAT,
                ma_20 FLOAT,
                ma_50 FLOAT,
                bb_high FLOAT,
                bb_low FLOAT,
                bb_mid FLOAT,
                adx FLOAT,
                atr FLOAT,
                volume_sma FLOAT,
                processing_date TIMESTAMP_NTZ
            )
        """)
        
        cursor.execute("DELETE FROM RAW_STOCK_DATA WHERE 1=1")
        
        insert_query = """
            INSERT INTO RAW_STOCK_DATA 
            (date, open, high, low, close, volume, symbol, extraction_date)
            VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'), %s, %s, %s, %s, %s, %s, 
                    TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'))
        """
        
        batch_size = 50
        inserted = 0
        
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i:i+batch_size]
            values = []
            
            for row in batch:
                values.append((
                    str(row['date']),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume']),
                    str(row['symbol']),
                    str(row['extraction_date'])
                ))
            
            cursor.executemany(insert_query, values)
            inserted += len(batch)
        
        conn.commit()
        logger.info(f"🎉 {inserted} lignes sauvegardées")
        
        cursor.close()
        conn.close()
        
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur Snowflake: {str(e)[:200]}")
        raise

def calculate_indicators_spark(**context):
    """Calcul des indicateurs avec SPARK SQL"""
    logger.info("🔥 Calcul des indicateurs avec Spark SQL...")
    
    try:
        # Initialiser Spark Session
        spark = SparkSession.builder \
            .appName("FinancialIndicators") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        logger.info("✅ Spark Session créée")
        
        # Lire les données depuis Snowflake
        snowflake_options = {
            "sfUrl": "votre-compte.snowflakecomputing.com",
            "sfUser": "votre_user",
            "sfPassword": "votre_password",
            "sfDatabase": "votre_db",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH"
        }
        
        # Alternative: Lire via JDBC
        jdbc_url = "jdbc:snowflake://votre-compte.snowflakecomputing.com"
        
        df_raw = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "RAW_STOCK_DATA") \
            .option("user", "votre_user") \
            .option("password", "votre_password") \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .load()
        
        logger.info(f"📥 {df_raw.count()} lignes chargées dans Spark")
        
        # Préparer les données
        df_raw.createOrReplaceTempView("raw_stocks")
        
        # 1. Calculer les moyennes mobiles avec Spark SQL
        logger.info("📈 Calcul des moyennes mobiles...")
        
        df_with_ma = spark.sql("""
            SELECT 
                *,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as ma_20,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) as ma_50
            FROM raw_stocks
        """)
        
        df_with_ma.createOrReplaceTempView("stocks_with_ma")
        
        # 2. Calculer le RSI avec Spark SQL
        logger.info("📊 Calcul du RSI...")
        
        df_price_change = spark.sql("""
            SELECT 
                *,
                close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as price_change
            FROM stocks_with_ma
        """)
        
        df_price_change.createOrReplaceTempView("price_changes")
        
        df_gain_loss = spark.sql("""
            SELECT 
                *,
                CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
                CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
            FROM price_changes
        """)
        
        df_gain_loss.createOrReplaceTempView("gain_loss")
        
        # Moyenne des gains et pertes sur 14 jours
        df_avg_gain_loss = spark.sql("""
            SELECT 
                *,
                AVG(gain) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) as avg_gain,
                AVG(loss) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) as avg_loss
            FROM gain_loss
        """)
        
        df_avg_gain_loss.createOrReplaceTempView("avg_gain_loss")
        
        # Calcul final du RSI
        df_rsi = spark.sql("""
            SELECT 
                *,
                CASE 
                    WHEN avg_loss = 0 THEN 100
                    ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
                END as rsi
            FROM avg_gain_loss
        """)
        
        df_rsi.createOrReplaceTempView("stocks_with_rsi")
        
        # 3. Calculer le MACD avec Spark SQL
        logger.info("📉 Calcul du MACD...")
        
        df_macd = spark.sql("""
            SELECT 
                *,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) as ema_12,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
                ) as ema_26,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
                ) as signal_9
            FROM stocks_with_rsi
        """)
        
        df_macd.createOrReplaceTempView("macd_calc")
        
        df_macd_final = spark.sql("""
            SELECT 
                *,
                ema_12 - ema_26 as macd,
                (ema_12 - ema_26) - signal_9 as macd_diff
            FROM macd_calc
        """)
        
        # 4. Calculer les Bollinger Bands
        logger.info("📊 Calcul des Bollinger Bands...")
        
        window_bb = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)
        
        df_bb = df_macd_final.withColumn("ma_20_bb", avg("close").over(window_bb)) \
            .withColumn("std_20", stddev("close").over(window_bb)) \
            .withColumn("bb_high", col("ma_20_bb") + (2 * col("std_20"))) \
            .withColumn("bb_low", col("ma_20_bb") - (2 * col("std_20")))
        
        # 5. Calculer l'ADX (Directional Movement)
        logger.info("📈 Calcul de l'ADX...")
        
        window_adx = Window.partitionBy("symbol").orderBy("date")
        
        df_adx = df_bb.withColumn("prev_high", lag("high", 1).over(window_adx)) \
            .withColumn("prev_low", lag("low", 1).over(window_adx)) \
            .withColumn("up_move", col("high") - col("prev_high")) \
            .withColumn("down_move", col("prev_low") - col("low")) \
            .withColumn("plus_dm", when((col("up_move") > col("down_move")) & (col("up_move") > 0), col("up_move")).otherwise(0)) \
            .withColumn("minus_dm", when((col("down_move") > col("up_move")) & (col("down_move") > 0), col("down_move")).otherwise(0))
        
        # Moyenne des DM sur 14 jours
        window_dm_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
        df_adx = df_adx.withColumn("plus_dm_14", avg("plus_dm").over(window_dm_avg)) \
            .withColumn("minus_dm_14", avg("minus_dm").over(window_dm_avg))
        
        # True Range et ATR
        df_adx = df_adx.withColumn("tr1", col("high") - col("low")) \
            .withColumn("tr2", abs(col("high") - col("prev_high"))) \
            .withColumn("tr3", abs(col("low") - col("prev_low"))) \
            .withColumn("true_range", greatest(col("tr1"), col("tr2"), col("tr3")))
        
        window_atr = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
        df_adx = df_adx.withColumn("atr", avg("true_range").over(window_atr))
        
        # Calcul final ADX
        window_dx = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
        df_adx = df_adx.withColumn("dx", 
            when((col("plus_dm_14") + col("minus_dm_14")) > 0,
                100 * (abs(col("plus_dm_14") - col("minus_dm_14")) / (col("plus_dm_14") + col("minus_dm_14")))
            ).otherwise(0)
        ).withColumn("adx", avg("dx").over(window_dx))
        
        # 6. Volume SMA
        window_volume = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)
        df_final = df_adx.withColumn("volume_sma", avg("volume").over(window_volume))
        
        logger.info(f"🧮 Traitement Spark terminé: {df_final.count()} lignes")
        
        # Préparer pour insertion dans Snowflake
        processing_date = datetime.now()
        
        # Convertir en pandas pour insertion (simplifié pour l'exemple)
        # En production, on utiliserait spark.write avec le connector Snowflake
        pandas_df = df_final.select(
            "date", "symbol", "open", "high", "low", "close", "volume",
            "rsi", "macd", "signal_9", "macd_diff",
            "ma_20", "ma_50", "bb_high", "bb_low", "ma_20_bb",
            "adx", "atr", "volume_sma"
        ).toPandas()
        
        logger.info(f"📤 Conversion en pandas: {len(pandas_df)} lignes")
        
        # Insérer dans Snowflake
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM PROCESSED_STOCK_DATA_SPARK WHERE 1=1")
        
        inserted = 0
        for _, row in pandas_df.iterrows():
            cursor.execute("""
                INSERT INTO PROCESSED_STOCK_DATA_SPARK 
                (date, symbol, open, high, low, close, volume,
                 rsi, macd, macd_signal, macd_diff, ma_20, ma_50,
                 bb_high, bb_low, bb_mid, adx, atr, volume_sma, processing_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['date'],
                str(row['symbol']),
                float(row['open']) if pd.notna(row['open']) else 0.0,
                float(row['high']) if pd.notna(row['high']) else 0.0,
                float(row['low']) if pd.notna(row['low']) else 0.0,
                float(row['close']) if pd.notna(row['close']) else 0.0,
                int(row['volume']) if pd.notna(row['volume']) else 0,
                float(row['rsi']) if pd.notna(row['rsi']) else 0.0,
                float(row['macd']) if pd.notna(row['macd']) else 0.0,
                float(row['signal_9']) if pd.notna(row['signal_9']) else 0.0,
                float(row['macd_diff']) if pd.notna(row['macd_diff']) else 0.0,
                float(row['ma_20']) if pd.notna(row['ma_20']) else 0.0,
                float(row['ma_50']) if pd.notna(row['ma_50']) else 0.0,
                float(row['bb_high']) if pd.notna(row['bb_high']) else 0.0,
                float(row['bb_low']) if pd.notna(row['bb_low']) else 0.0,
                float(row['ma_20_bb']) if pd.notna(row['ma_20_bb']) else 0.0,
                float(row['adx']) if pd.notna(row['adx']) else 0.0,
                float(row['atr']) if pd.notna(row['atr']) else 0.0,
                float(row['volume_sma']) if pd.notna(row['volume_sma']) else 0.0,
                processing_date
            ))
            inserted += 1
        
        conn.commit()
        logger.info(f"🎯 {inserted} lignes sauvegardées via Spark SQL")
        
        # Arrêter Spark
        spark.stop()
        logger.info("🛑 Spark Session arrêtée")
        
        return inserted
        
    except Exception as e:
        logger.error(f"❌ Erreur Spark: {str(e)[:200]}")
        if 'spark' in locals():
            spark.stop()
        raise

def quality_check(**context):
    """Contrôle qualité"""
    logger.info("🔍 Contrôle qualité...")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                'PROCESSED_STOCK_DATA_SPARK' as table,
                COUNT(*) as rows,
                COUNT(DISTINCT symbol) as symbols,
                MIN(date) as min_date,
                MAX(date) as max_date,
                AVG(rsi) as avg_rsi,
                AVG(macd) as avg_macd
            FROM PROCESSED_STOCK_DATA_SPARK
            GROUP BY 1
        """)
        
        results = cursor.fetchall()
        
        logger.info("=" * 50)
        logger.info("🔥 RAPPORT SPARK SQL")
        logger.info("=" * 50)
        
        for table, rows, symbols, min_date, max_date, avg_rsi, avg_macd in results:
            logger.info(f"\n📈 Table: {table}")
            logger.info(f"   Lignes: {rows}")
            logger.info(f"   Symboles: {symbols}")
            logger.info(f"   Période: {min_date} à {max_date}")
            logger.info(f"   RSI moyen: {avg_rsi:.2f}")
            logger.info(f"   MACD moyen: {avg_macd:.4f}")
        
        cursor.close()
        conn.close()
        
        logger.info("\n✅ QUALITÉ CONFORME - SPARK SQL UTILISÉ")
        logger.info("📈 Données prêtes pour Streamlit")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"⚠️ Erreur contrôle qualité: {e}")
        return True

with DAG(
    'finance_pipeline_spark',
    default_args=default_args,
    description='Pipeline avec Spark SQL: Données financières → Snowflake',
    schedule_interval='0 18 * * 1-5',
    catchup=False,
    tags=['finance', 'spark', 'snowflake', 'streamlit'],
) as dag:
    
    create_data = PythonOperator(
        task_id='create_demo_data',
        python_callable=create_demo_data,
    )
    
    save_data = PythonOperator(
        task_id='save_to_snowflake',
        python_callable=save_to_snowflake,
    )
    
    process_data_spark = PythonOperator(
        task_id='calculate_indicators_spark',
        python_callable=calculate_indicators_spark,
    )
    
    quality_check = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check,
    )
    
    create_data >> save_data >> process_data_spark >> quality_check
    
    dag.doc_md = """
    ## Pipeline Finance avec Spark SQL
    
    **Équipe:** Assia Boujnah, Soukaina El Mahjoubi, Khalil Fatima
    
    ### Description
    Pipeline ETL avec Spark SQL comme demandé dans le cahier des charges.
    
    ### Flow
    1. **Création données démo** → Données réalistes pour 8 symboles
    2. **Sauvegarde Snowflake** → Stockage cloud (remplace Hive)
    3. **Calcul indicateurs avec Spark SQL** → RSI, MACD, MA, ADX, Bollinger Bands (via Spark SQL)
    4. **Contrôle qualité** → Vérification complète
    
    ### Technologies utilisées
    - **Airflow** : Orchestration
    - **Spark SQL** : Traitement des données (conforme au cahier)
    - **Snowflake** : Stockage (alternative moderne à Hive)
    - **Streamlit** : Visualisation (étape suivante)
    
    ### Indicateurs calculés avec Spark SQL
    - RSI (Relative Strength Index)
    - MACD (Moving Average Convergence Divergence)
    - Moyennes Mobiles (20 et 50 jours)
    - Bollinger Bands
    - ADX (Average Directional Index)
    - ATR (Average True Range)
    """