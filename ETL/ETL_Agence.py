import pandas as pd
import psycopg2

# Charger les données source
file_path = "C:/Users/feres/Downloads/agences-_1_-_1_-_1_.csv"
df = pd.read_csv(file_path, sep=",", header=None, encoding="utf-8", dtype=str)

# Définir les en-têtes
df.columns = ['agence', 'libelle', 'structure_mere', 'dirRegionale']
df = df[1:].reset_index(drop=True)

# 🔍 Affichage des données avant transformation
print("📌 Données avant transformation :")
print(df.head())

# 🌟 Création de la DataFrame pour `dim_dir_regionale`
df_dir_regionale = df[['dirRegionale']].drop_duplicates().reset_index(drop=True)
df_dir_regionale['id_dir_regionale'] = df_dir_regionale.index + 1  # ID unique

# 🔍 Affichage des données `dim_dir_regionale`
print("\n✅ Données de la dimension dim_dir_regionale :")
print(df_dir_regionale.head())

# 🌟 Création de la DataFrame pour `dim_agence`
# Associer chaque `structure_mere` à `id_dir_regionale` (via `dirRegionale`)
df['id_dir_regionale'] = df['dirRegionale'].map(df_dir_regionale.set_index('dirRegionale')['id_dir_regionale'])
df_agence = df[['agence', 'libelle', 'structure_mere', 'id_dir_regionale']].copy()

# Renommer les colonnes pour correspondre à la structure de la dimension `dim_agence`
df_agence.columns = ['id_agence', 'libelle_agence', 'code_agence', 'id_dir_regionale']

# 🔍 Affichage des données `dim_agence`
print("\n✅ Données de la dimension dim_agence :")
print(df_agence.head())

# Connexion à PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="datawarehauseBNA",
        user="postgres",
        password="1234",
        host="localhost",
        port="5435",
        client_encoding='UTF8'
    )
    print("\n✅ Connexion réussie à la base de données PostgreSQL.")
except psycopg2.OperationalError as e:
    print(f"\n❌ Erreur de connexion : {e}")
    exit()

cur = conn.cursor()

# 🔄 Insertion des données dans `dim_dir_regionale` (UPSERT pour éviter les doublons)
for index, row in df_dir_regionale.iterrows():
    cur.execute("""
        INSERT INTO dim_dir_regionale (id_dir_regionale, libelle_dir_regionale)
        VALUES (%s, %s)
        ON CONFLICT (id_dir_regionale)
        DO UPDATE SET libelle_dir_regionale = EXCLUDED.libelle_dir_regionale
    """, (int(row['id_dir_regionale']), row['dirRegionale']))

# 🔄 Insertion des données dans `dim_agence` (UPSERT pour éviter les doublons)
for index, row in df_agence.iterrows():
    # Vérification que l'id_dir_regionale existe dans dim_dir_regionale
    cur.execute("""
        SELECT 1 FROM dim_dir_regionale WHERE id_dir_regionale = %s
    """, (int(row['id_dir_regionale']),))
    
    if cur.fetchone():  # Si l'ID existe dans dim_dir_regionale
        cur.execute("""
            INSERT INTO dim_agence (id_agence, libelle_agence, code_agence, id_dir_regionale)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_agence)
            DO UPDATE SET libelle_agence = EXCLUDED.libelle_agence,
                          code_agence = EXCLUDED.code_agence,
                          id_dir_regionale = EXCLUDED.id_dir_regionale
        """, (int(row['id_agence']), row['libelle_agence'], int(row['code_agence']), int(row['id_dir_regionale'])))
    else:
        print(f"⚠️ L'ID {row['id_dir_regionale']} n'existe pas dans dim_dir_regionale.")

# 💾 Valider et fermer la connexion
conn.commit()
cur.close()
conn.close()

print("\n✅ Données insérées avec succès dans les dimensions dim_agence et dim_dir_regionale !")
