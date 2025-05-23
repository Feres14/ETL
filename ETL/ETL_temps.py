# ETL_temps_CORRIGE.py
import pandas as pd
import psycopg2
from datetime import datetime
import locale

# --- Configuration Locale ---
try:
    locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'French_France.1252')
    except locale.Error:
        print("[WARN] Locale fr_FR non trouvée. Noms mois/jour en anglais.")

# --- Fonction robuste de conversion de date ---
def convertir_date_robuste(date_str):
    if pd.isna(date_str) or date_str == '': return pd.NaT
    for fmt in ('%d/%m/%Y', '%d/%m/%y'):
        try: return pd.to_datetime(date_str, format=fmt, errors='raise')
        except ValueError: continue
    return pd.NaT

# --- Connexion DB ---
print(f"--- Début ETL dim_temps ({datetime.now()}) ---")
try:
    conn = psycopg2.connect(
        dbname="datawarehauseBNA", user="postgres", password="1234", # VOS INFOS
        host="localhost", port="5435", client_encoding='UTF8'
    )
    cursor = conn.cursor()
    print("✅ Connexion PostgreSQL réussie.")
except Exception as e: print(f"❌ Erreur connexion : {e}"); exit()

# --- Chargement des données ---
print("⏳ Chargement des fichiers CSV...")
df_canevas_raw = df_demande_raw = None # Initialiser
try:
    def clean_col_names(df):
        if df is None or df.empty: return df
        df.columns = [str(col).strip().replace('"', '').replace(' ', '_') for col in df.columns]
        first_col = df.columns[0] if not df.empty else None
        if first_col and (df[first_col].astype(str).str.match(r'^\s*$', na=False) | df[first_col].astype(str).fillna('').eq('')).all():
             if len(df.columns) > 1: df = df.iloc[:, 1:]
        return df

    df_canevas_raw = pd.read_csv("C:/Users/feres/Downloads/CANEVAS_RETAILS.csv", dtype=str, low_memory=False,skiprows=3)
    if df_canevas_raw is not None: df_canevas_raw = clean_col_names(df_canevas_raw)

    df_demande_raw = pd.read_csv("C:/Users/feres/Downloads/DEMANDE_CG_2018_2019_2020-_1_.csv", dtype=str, low_memory=False, skiprows=0)
    if df_demande_raw is not None: df_demande_raw = clean_col_names(df_demande_raw)

except Exception as e: print(f"❌ Erreur chargement initial CSV : {e}"); exit()

# --- Extraction et Nettoyage des Dates ---
all_dates_list = []
print("⚙️  Extraction des dates...")

# Dates Canevas (Adapter noms si nettoyage différent)
cols_dates_canevas = ['Date_Création', 'Date_Naissance', 'Date_MEP']
if df_canevas_raw is not None:
    for col in cols_dates_canevas:
        if col in df_canevas_raw.columns:
            all_dates_list.extend(df_canevas_raw[col].dropna().tolist()) # dropna ici pour éviter erreurs
        else: print(f"  [WARN] Colonne '{col}' non trouvée dans Canevas.")

# Dates Demande (Adapter noms si nettoyage différent)
cols_dates_demande = ["DAT_DEM_DECCG", "DAT_VALDR_DECCG", "DAT_VALCCI_DECCG", "DAT_MEP_DECCG"]
if df_demande_raw is not None:
    for col in cols_dates_demande:
        if col in df_demande_raw.columns:
            all_dates_list.extend(df_demande_raw[col].dropna().tolist())
        else: print(f"  [WARN] Colonne '{col}' non trouvée dans Demande.")

if not all_dates_list: print("❌ Aucune date non-vide extraite."); exit()

# --- Création DataFrame dates uniques ---
print(f"⚙️  Traitement dates...")
dates_series = pd.Series(all_dates_list)
dates_series_dt = dates_series.apply(convertir_date_robuste)
unique_dates = pd.Series(dates_series_dt.dropna().unique())

if len(unique_dates) == 0: print("❌ Aucune date VALIDE trouvée."); exit()

dim_temps_df = pd.DataFrame({'date': unique_dates})
dim_temps_df = dim_temps_df.sort_values('date').reset_index(drop=True)
print(f"   -> {len(dim_temps_df)} dates uniques valides trouvées.")
print(f"   -> Plage: {dim_temps_df['date'].min():%Y-%m-%d} à {dim_temps_df['date'].max():%Y-%m-%d}")

# --- Extraction attributs ---
print("⚙️  Extraction attributs temporels...")
dim_temps_df['jour'] = dim_temps_df['date'].dt.day_name() # Nom du jour
dim_temps_df['mois'] = dim_temps_df['date'].dt.month_name() # Nom du mois
dim_temps_df['annee'] = dim_temps_df['date'].dt.year
dim_temps_df['trimestre'] = dim_temps_df['date'].dt.quarter # Numéro trimestre
dim_temps_df['mois_numero'] = dim_temps_df['date'].dt.month # Numéro mois
dim_temps_df['jour_numero'] = dim_temps_df['date'].dt.day # Numéro jour
dim_temps_df['id_temps'] = range(1, len(dim_temps_df) + 1)

# Ordre final colonnes
colonnes_finales = ['id_temps', 'date', 'jour', 'mois', 'annee', 'trimestre', 'mois_numero', 'jour_numero']
dim_temps_df = dim_temps_df[colonnes_finales]

# --- Insertion ---
try:
    print("⏳ TRUNCATE TABLE public.dim_temps...")
    cursor.execute("TRUNCATE TABLE public.dim_temps RESTART IDENTITY CASCADE;")
    conn.commit()

    print(f"⏳ Insertion de {len(dim_temps_df)} lignes dans dim_temps...")
    placeholders = ', '.join(['%s'] * len(colonnes_finales))
    insert_query = f"INSERT INTO public.dim_temps ({', '.join(colonnes_finales)}) VALUES ({placeholders})"
    data_to_insert = [tuple(row) for row in dim_temps_df.itertuples(index=False)]

    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(" Table dim_temps remplie avec succès !")
except Exception as e:
    conn.rollback()
    print(f" Erreur insertion dim_temps : {e}")
    if hasattr(e, 'pgerror'): print(f"   -> PGError: {e.pgerror}")
finally:
    if cursor: cursor.close()
    if conn: conn.close()
    print("🔌 Connexion fermée.")
    print(f"--- Fin ETL dim_temps ({datetime.now()}) ---")