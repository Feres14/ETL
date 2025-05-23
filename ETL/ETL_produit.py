import pandas as pd
import psycopg2

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
    print("\n✅ Connexion réussie à PostgreSQL.")
except psycopg2.OperationalError as e:
    print(f"\n❌ Erreur de connexion : {e}")
    exit()

cursor = conn.cursor()

# Charger les fichiers CSV
file_produit = "C:/Users/feres/Downloads/produit-_1_-_1_.csv"
file_canevas = "C:/Users/feres/Downloads/CANEVAS_RETAILS.csv"

df_produit = pd.read_csv(file_produit, sep=",", dtype=str, encoding="utf-8").fillna('')
df_canevas = pd.read_csv(file_canevas, sep=",", dtype=str, encoding="utf-8").fillna('')

# 🔹 Nettoyage des en-têtes CANEVAS
header_row = df_canevas[df_canevas.astype(str).apply(lambda x: x.str.contains("CANEVAS", na=False)).any(axis=1)].index[0]
df_canevas.columns = df_canevas.iloc[header_row]
df_canevas = df_canevas[(header_row + 1):].reset_index(drop=True)

# 🔹 Nettoyage des codes produits
df_produit["CODE"] = df_produit["CODE"].astype(str).str.strip()
df_produit["LIBELLE"] = df_produit["LIBELLE"].astype(str).str.strip()
df_canevas["Code Produit"] = df_canevas["Code Produit"].astype(str).str.strip()

# 🔽 Insérer les familles de produits
familles = df_produit[['LIBELLE']].drop_duplicates().values.tolist()
famille_ids = {}

for famille in familles:
    cursor.execute(
        "INSERT INTO dim_famille_produit (libelle_famille_produit) VALUES (%s) ON CONFLICT (libelle_famille_produit) DO NOTHING RETURNING id_famille_produit",
        (famille[0],)
    )
    result = cursor.fetchone()
    famille_ids[famille[0]] = result[0] if result else None

# 🔽 Insérer les sous-produits
sous_produits = df_produit[['LIBELLE']].drop_duplicates().values.tolist()
sous_produit_ids = {}

for sous_produit in sous_produits:
    id_famille = famille_ids.get(sous_produit[0])
    cursor.execute(
        "INSERT INTO dim_sous_produit (id_famille_produit, libelle_sous_produit) VALUES (%s, %s) ON CONFLICT (libelle_sous_produit) DO NOTHING RETURNING id_sous_produit",
        (id_famille, sous_produit[0])
    )
    result = cursor.fetchone()
    sous_produit_ids[sous_produit[0]] = result[0] if result else None

# 🔽 Insérer les produits
processed_codes = set()
error_count = 0
display_limit = 5  # 🔹 Afficher uniquement les 5 premières lignes

for _, row in df_canevas.iterrows():
    code_produit = str(row["Code Produit"]).strip()

    # Vérification si code_produit est vide ou déjà traité
    if not code_produit or code_produit in processed_codes:
        continue

    processed_codes.add(code_produit)

    # 🔍 Correction : Rechercher le libellé du produit
    libelle_match = df_produit[df_produit["CODE"] == code_produit]["LIBELLE"].values
    libelle_produit = libelle_match[0] if len(libelle_match) > 0 else "Produit Inconnu"

    # 🔍 Correction : Vérifier si le sous-produit existe 
    id_sous_produit = sous_produit_ids.get(libelle_produit)

    if id_sous_produit is None:
        error_count += 1
        if error_count <= display_limit:
            print(f"⚠️ Produit '{libelle_produit}' ({code_produit}) sans sous-produit associé.")

    if len(processed_codes) <= display_limit:
        print(f"🔹 Code Produit: {code_produit}, Libellé: {libelle_produit}, ID Sous Produit: {id_sous_produit}")

    cursor.execute(
        """
        INSERT INTO dim_produit (id_sous_produit, libelle_produit, code_produit)
        VALUES (%s, %s, %s)
        ON CONFLICT (code_produit) DO NOTHING
        """,
        (id_sous_produit, libelle_produit, code_produit)
    )

# 🔽 Valider et fermer la connexion
conn.commit()
cursor.close()
conn.close()

print("\n✅ Données insérées avec succès ! 🚀")
if error_count > display_limit:
    print(f"\n⚠️ {error_count - display_limit} erreurs supplémentaires non affichées.")
