# config/spark_config.py
import os
from pyspark.sql import SparkSession

class SparkConfig:
    """Configuration Spark pour votre projet"""
    
    # Vos informations Snowflake
    SNOWFLAKE_CONFIG = {
        "sfUrl": "eqqseml-wv78446.snowflakecomputing.com",
        "sfUser": "SOUKAINA",
        "sfPassword": "soso123456SOSO123456@",
        "sfDatabase": "FINANCE_DB",
        "sfSchema": "RAW_DATA",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "FINANCE_ROLE"
    }
    
    # Configuration Spark
    SPARK_CONFIG = {
        "spark.app.name": "FinancialDataProcessor",
        "spark.master": "local[*]",  # Mode local pour développement
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        "spark.executor.cores": "2",
        "spark.sql.shuffle.partitions": "4",
        "spark.jars": f"{os.getenv('AIRFLOW_HOME', '/opt/airflow')}/spark_jars/spark-snowflake_2.12-2.11.0-spark_3.3.jar",
        "spark.jars.packages": "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3"
    }
    
    @classmethod
    def create_spark_session(cls, app_name=None):
        """Crée une session Spark configurée"""
        
        # Mettre à jour le nom de l'application si fourni
        config = cls.SPARK_CONFIG.copy()
        if app_name:
            config["spark.app.name"] = app_name
        
        # Créer la session Spark
        spark = SparkSession.builder \
            .appName(config["spark.app.name"]) \
            .master(config["spark.master"]) \
            .config("spark.executor.memory", config["spark.executor.memory"]) \
            .config("spark.driver.memory", config["spark.driver.memory"]) \
            .config("spark.executor.cores", config["spark.executor.cores"]) \
            .config("spark.sql.shuffle.partitions", config["spark.sql.shuffle.partitions"]) \
            .config("spark.jars", config["spark.jars"]) \
            .config("spark.jars.packages", config["spark.jars.packages"]) \
            .getOrCreate()
        
        # Configurer les logs
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @classmethod
    def get_snowflake_options(cls, table_name=None, custom_schema=None):
        """Retourne les options Snowflake pour Spark"""
        options = cls.SNOWFLAKE_CONFIG.copy()
        
        if table_name:
            options["dbtable"] = table_name
        
        if custom_schema:
            options["sfSchema"] = custom_schema
        
        return options