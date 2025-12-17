import snowflake.connector
import pandas as pd

print("🧪 Test de connexion Snowflake...")

try:
    # 1. CONNEXION
    conn = snowflake.connector.connect(
        user='SOUKAINA',
        password='soso123456SOSO123456@',
        account='eqqseml-wv78446',
        warehouse='COMPUTE_WH',
        database='FINANCE_DB',
        schema='SOUKAINA_SCHEMA'
    )
    
    print("✅ Étape 1/3 : Connexion réussie !")
    
    # 2. TEST DE LA TABLE
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM PROCESSED_DATA")
    result = cursor.fetchone()
    print(f"✅ Étape 2/3 : Table PROCESSED_DATA trouvée !")
    print(f"   Nombre de lignes : {result[0]:,}")
    
    # 3. TEST DES COLONNES
    cursor.execute("SELECT * FROM PROCESSED_DATA LIMIT 0")  # Juste les métadonnées
    cols = [desc[0] for desc in cursor.description]
    print(f"✅ Étape 3/3 : {len(cols)} colonnes trouvées")
    print(f"   Colonnes (10 premières) : {cols[:10]}")
    
    # 4. TEST D'UN ÉCHANTILLON
    print("\n📊 Échantillon de données (5 premières lignes) :")
    query = "SELECT * FROM PROCESSED_DATA LIMIT 5"
    df = pd.read_sql(query, conn)
    print(df)
    
    conn.close()
    print("\n🎉 Tous les tests sont réussis !")
    
except snowflake.connector.errors.DatabaseError as e:
    print(f"❌ ERREUR DATABASE : {e}")
    print("Vérifiez :")
    print("1. Vos identifiants (compte, utilisateur, mot de passe)")
    print("2. Que la table PROCESSED_DATA existe bien")
    print("3. Votre connexion Internet")
    
except Exception as e:
    print(f"❌ ERREUR GÉNÉRALE : {e}")
    print(f"Type d'erreur : {type(e).__name__}")