import psycopg2
import pandas as pd

# 📂 Charger les données
file_path = "C:/Users/feres/Downloads/CANEVAS_RETAILS.csv"
df = pd.read_csv(file_path, sep=",", header=None, encoding="utf-8", dtype=str)

# 🔍 Trouver la ligne des en-têtes
header_row = df[df.astype(str).apply(lambda x: x.str.contains("CANEVAS", na=False)).any(axis=1)].index[0]

# 🏗️ Définir les en-têtes correctes
df.columns = df.iloc[header_row]
df = df[(header_row + 1):].reset_index(drop=True)

# 🔍 Vérifier les colonnes disponibles
print("📌 Colonnes disponibles :", df.columns.tolist())

# 🌟 Sélectionner et renommer les colonnes pour `dim_sort_credit`
df_sort_credit = df[['Décision Finale']].copy()
df_sort_credit.rename(columns={'Décision Finale': 'sort_credit'}, inplace=True)

# 🏷️ Assigner un ID unique **séquentiel** pour toutes les lignes (pas seulement les valeurs uniques)
df_sort_credit['id_decision'] = df_sort_credit.index + 1  # Index commence à 0, on ajoute 1

# 🔍 Affichage des données après transformation
print("\n✅ Données après transformation :")
print(df_sort_credit.head())

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

# 🔄 Insertion des données dans PostgreSQL (ajout de toutes les lignes)
for index, row in df_sort_credit.iterrows():
    cur.execute("""
        INSERT INTO dim_sort_credit (id_decision, sort_credit) 
        VALUES (%s, %s)
    """, (row['id_decision'], row['sort_credit']))

# 💾 Valider et fermer la connexion
conn.commit()
cur.close()
conn.close()

print("\n✅ Toutes les données ont été insérées dans `dim_sort_credit` avec succès !")
