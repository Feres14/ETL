import psycopg2
import pandas as pd

# 📂 Charger les données transformées
file_path = "C:/Users/feres/Downloads/CANEVAS_RETAILS.csv"
df = pd.read_csv(file_path, sep=",", header=None, encoding="utf-8", dtype=str)

# 🔍 Trouver la ligne où commencent les en-têtes
header_row = df[df.astype(str).apply(lambda x: x.str.contains("CANEVAS", na=False)).any(axis=1)].index[0]

# 🏗️ Définir les en-têtes correctes
df.columns = df.iloc[header_row]
df = df[(header_row + 1):].reset_index(drop=True)

# 🧐 Vérifier les colonnes avant transformation
print("🔍 Colonnes avant transformation :", df.columns.tolist())

# 🌟 Transformation pour `dim_fiche_d_etude`
df_fiche = df[['N°CANEVAS', 'Montant Sollicité', 'Retenus Mensuel', 'Revenus Annuel']].copy()

# ✅ Renommage correct des colonnes
df_fiche.rename(columns={
    'N°CANEVAS': 'id_fiche_etude',
    'Montant Sollicité': 'montant_sollicite',
    'Retenus Mensuel': 'revenus_mensuels',  # ⚠️ Assure-toi que c'est bien "Retenus Mensuel" et pas une autre variante
    'Revenus Annuel': 'revenus_annuel'
}, inplace=True)

# 🧐 Vérification après renommage
print("✅ Colonnes après renommage :", df_fiche.columns.tolist())

# 🔄 Nettoyage des données : suppression des virgules et conversion en float
def clean_numeric(value):
    """ Supprime les virgules et convertit en float si possible. """
    try:
        return float(value.replace(',', '').strip()) if pd.notna(value) else None
    except ValueError:
        return None

df_fiche['montant_sollicite'] = df_fiche['montant_sollicite'].apply(clean_numeric)
df_fiche['revenus_mensuels'] = df_fiche['revenus_mensuels'].apply(clean_numeric)
df_fiche['revenus_annuel'] = df_fiche['revenus_annuel'].apply(clean_numeric)

# 🔍 Vérification des données après transformation
print("\n✅ Données après transformation :")
print(df_fiche.head())

# 🔗 Connexion à PostgreSQL
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

# 🔄 Insertion des données dans PostgreSQL (UPSERT pour éviter les doublons)
try:
    for index, row in df_fiche.iterrows():
        cur.execute(""" 
            INSERT INTO dim_fiche_d_etude (id_fiche_etude, montant_solicite, revenus_mensuels, revenus_annuel) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_fiche_etude) 
            DO UPDATE SET 
                montant_solicite = EXCLUDED.montant_solicite, 
                revenus_mensuels = EXCLUDED.revenus_mensuels, 
                revenus_annuel = EXCLUDED.revenus_annuel
        """, (row['id_fiche_etude'], row['montant_sollicite'], row['revenus_mensuels'], row['revenus_annuel']))

    # 💾 Valider et fermer la connexion
    conn.commit()
    print("\n✅ Données insérées dans dim_fiche_d_etude avec succès !")

except psycopg2.Error as e:
    print(f"\n❌ Erreur d'insertion dans PostgreSQL : {e}")
    conn.rollback()

finally:
    cur.close()
    conn.close()
    print("\n🔒 Connexion fermée.")
