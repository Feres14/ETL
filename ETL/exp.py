#-*- coding: utf-8 -*-
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import io
from decimal import Decimal, getcontext, ROUND_HALF_UP
import random
import re
#=============================================================================
#== Script ETL v11.6 - Jointure sur Libellé Agence pour faits_decision      ==
#=============================================================================
#Objectif: Corriger la jointure de faits_decision en utilisant le nom de l'agence.
#=============================================================================
print(f"--- Début du script ETL Faits (Agrégation Finale v11.6 - Jointure Libellé) ({datetime.now()}) ---")
#✅ Connexion (inchangée)
try:
     conn = psycopg2.connect(
          dbname="datawarehauseBNA", user="postgres", password="1234", # <--- VOS INFOS
         host="localhost", port="5435", client_encoding='UTF8'
     )
     cursor = conn.cursor()
     print("✅ Connexion à PostgreSQL réussie.")
except Exception as e: print(f"❌ Erreur connexion: {e}"); exit()
#--- Helper functions (inchangées) ---
placeholder = 'INCONNU'
#... (clean_numeric, charger_donnees, convertir_date, get_any_id - utiliser les versions précédentes) ...
def clean_numeric(value): # Utiliser la version v11.3 qui fonctionnait pour canevas
     if pd.isna(value): return np.nan
     if isinstance(value, (int, float, Decimal)): return float(value)
     original_value_repr = repr(value); cleaned_value = str(value).strip()
     if not cleaned_value: return np.nan
     cleaned_value = cleaned_value.replace(',', '.')
     parts = cleaned_value.split('.')
     if len(parts) > 1: integer_part = "".join(parts[:-1]); cleaned_value = integer_part + "." + parts[-1]
     else: cleaned_value = parts[0]
     try: return float(cleaned_value)
     except ValueError: return np.nan
     except Exception: return np.nan
def charger_donnees(chemin, dtype=str, header_keyword='CANEVAS', skip_initial_rows=3):
    print(f"⏳ Chargement: {chemin}...")
    try:
        df = pd.read_csv(chemin, sep=",", dtype=dtype, low_memory=False, keep_default_na=False, encoding='utf-8', skipinitialspace=True, skiprows=skip_initial_rows, on_bad_lines='warn')
        if df.empty: print(f"   -> Fichier vide {chemin}."); return pd.DataFrame()
        print(f"   -> {len(df)} lignes brutes chargées.")
        df.columns = [str(col).strip().replace('"', '').replace(' ', '_') for col in df.columns]
        first_col = df.columns[0];
        if (df[first_col].astype(str).str.match(r'^\s*$', na=False) | df[first_col].astype(str).fillna('').eq('')).all():
             if len(df.columns) > 1: df = df.iloc[:, 1:]
        print(f"✅ Chargé {chemin}. {len(df)}l, {len(df.columns)}c.")
        return df
    except FileNotFoundError: print(f"❌ Non trouvé - {chemin}"); return None
    except Exception as e: print(f"❌ Erreur chargement {chemin} : {e}"); return None
def convertir_date(date_str): # Inchangé
    if pd.isna(date_str) or date_str == '': return pd.NaT
    try: return pd.to_datetime(date_str, format='%d/%m/%y', errors='raise')
    except ValueError:
        try: return pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce')
        except Exception: return pd.NaT
def get_any_id(query): # Inchangé
    try: cursor.execute(query); result = cursor.fetchone(); return result[0] if result else None
    except Exception as e: print(f"❌ Erreur get_any_id query='{query}': {e}"); conn.rollback(); return None

# =============================================================================
# 1. CHARGEMENT ET PRÉPARATION
# =============================================================================
print("\n--- 1. Chargement et Préparation des Données ---")
df_canevas_raw = charger_donnees("C:/Users/feres/Downloads/CANEVAS_RETAILS.csv", dtype=str, header_keyword='CANEVAS', skip_initial_rows=3) # Charger même si on saute section 2 plus tard
df_demande_raw = charger_donnees("C:/Users/feres/Downloads/DEMANDE_CG_2018_2019_2020-_1_.csv", dtype=str, header_keyword='LIB_AGENCE', skip_initial_rows=0)

# --- Chargement dim_agence AVEC LIBELLE ---
dim_agences_df = pd.DataFrame()
print("⚙️  Chargement dim_agence (avec libelle)...")
try:
    # **** CHARGER id_agence ET libelle_agence ****
    cursor.execute("SELECT id_agence, code_agence, libelle_agence FROM dim_agence")
    dim_agences_list = cursor.fetchall()
    dim_agences_df = pd.DataFrame(dim_agences_list, columns=['id_agence', 'code_agence', 'libelle_agence'])

    # Nettoyage libelle pour la jointure
    dim_agences_df['libelle_agence_clean'] = dim_agences_df['libelle_agence'].fillna('').astype(str).str.strip().str.upper()
    # Garder une trace du code agence aussi, même si on ne joint pas dessus pour l'instant
    dim_agences_df['code_agence'] = pd.to_numeric(dim_agences_df['code_agence'], errors='coerce').astype('Int64')

    # Supprimer les lignes où le libellé nettoyé est vide
    initial_dim_rows = len(dim_agences_df)
    dim_agences_df.dropna(subset=['id_agence'], inplace=True) # Garder id_agence non nul
    dim_agences_df = dim_agences_df[dim_agences_df['libelle_agence_clean'] != '']
    print(f"   -> {initial_dim_rows} agences lues, {len(dim_agences_df)} conservées après nettoyage libellé.")
    # Vérifier les doublons sur le libellé nettoyé (problématique pour la jointure)
    if dim_agences_df.duplicated(subset=['libelle_agence_clean']).any():
        print(f"   -> !! ATTENTION: {dim_agences_df.duplicated(subset=['libelle_agence_clean']).sum()} libellés d'agence DUPLIQUÉS après nettoyage dans dim_agence !")
        print("      Exemples de doublons:", dim_agences_df[dim_agences_df.duplicated(subset=['libelle_agence_clean'], keep=False)]['libelle_agence_clean'].unique()[:5])
except Exception as e: print(f"❌ Erreur chargement dim_agence : {e}")

# --- Préparation df_canevas (simplifié, car fonctionnel) ---
df_canevas = None
if df_canevas_raw is not None and not df_canevas_raw.empty:
    df_canevas = df_canevas_raw.copy(); col_agence_canevas = 'Agence_Initiatrice'; col_revenu_annuel = 'Revenus_Annuel'; col_retenues = 'Retenus_Mensuel'; col_montant_sol = 'Montant_Sollicité'
    required_cols_canevas = [col_agence_canevas, col_revenu_annuel, col_retenues, col_montant_sol]
    if all(col in df_canevas.columns for col in required_cols_canevas):
        df_canevas['code_agence_int'] = pd.to_numeric(df_canevas[col_agence_canevas], errors='coerce').astype('Int64')
        df_canevas[col_revenu_annuel + '_num'] = df_canevas[col_revenu_annuel].apply(clean_numeric); df_canevas[col_retenues + '_num'] = df_canevas[col_retenues].apply(clean_numeric); df_canevas[col_montant_sol + '_num'] = df_canevas[col_montant_sol].apply(clean_numeric)
        df_canevas['capacite_row'] = (df_canevas[col_revenu_annuel + '_num'] - df_canevas[col_retenues + '_num'] * 12) * 0.40; df_canevas['taux_endettement_row'] = np.where((df_canevas[col_revenu_annuel + '_num'].notna()) & (df_canevas[col_revenu_annuel + '_num'] > 0), df_canevas[col_montant_sol + '_num'] / df_canevas[col_revenu_annuel + '_num'], np.nan); df_canevas['taux_endettement_row'].replace([np.inf, -np.inf], np.nan, inplace=True)
        df_canevas = df_canevas.dropna(subset=['code_agence_int']); print(f"✅ df_canevas préparé ({len(df_canevas)} lignes).")
        if df_canevas.empty: df_canevas = None
    else: df_canevas = None; print("❌ Cols manquantes df_canevas.")

# --- Préparation df_demande AVEC LIBELLE ---
df_demande = None
col_lib_agence_demande = 'LIB_AGENCE' # Utiliser le nom de l'agence
if df_demande_raw is not None and not df_demande_raw.empty:
    print(f"⚙️  Préparation df_demande (focus libellé agence)...")
    df_demande = df_demande_raw.copy()
    required_cols_dem = ['DAT_DEM_DECCG', 'DAT_VALDR_DECCG', 'DAT_VALCCI_DECCG', 'DAT_MEP_DECCG', col_lib_agence_demande]
    if not all(col in df_demande.columns for col in required_cols_dem):
         print(f"❌ Cols manquantes df_demande : {[col for col in required_cols_dem if col not in df_demande.columns]}.")
         df_demande = None
    else:
        # Nettoyage libelle pour la jointure
        df_demande['libelle_agence_demande_clean'] = df_demande[col_lib_agence_demande].fillna('').astype(str).str.strip().str.upper()

        # Calcul des délais (inchangé)
        date_cols_demande = ['DAT_DEM_DECCG', 'DAT_VALDR_DECCG', 'DAT_VALCCI_DECCG', 'DAT_MEP_DECCG']
        for col in date_cols_demande: df_demande[col] = df_demande[col].apply(convertir_date)
        date_pairs = [('duree_credit', 'DAT_MEP_DECCG', 'DAT_DEM_DECCG'), ('delai_validation', 'DAT_VALDR_DECCG', 'DAT_DEM_DECCG'), ('delai_mise_en_place', 'DAT_MEP_DECCG', 'DAT_VALCCI_DECCG'), ('delai_traitement', 'DAT_MEP_DECCG', 'DAT_DEM_DECCG')]
        for m, e, s in date_pairs:
             if e in df_demande.columns and s in df_demande.columns:
                 mask = df_demande[s].notna() & df_demande[e].notna(); df_demande[m] = pd.NA
                 df_demande.loc[mask, m] = (df_demande.loc[mask, e] - df_demande.loc[mask, s]).dt.days
                 df_demande[m] = pd.to_numeric(df_demande[m], errors='coerce').astype('Int64')
             else: df_demande[m] = pd.NA

        # Filtrer les lignes où le libellé nettoyé est vide
        initial_rows = len(df_demande)
        df_demande = df_demande[df_demande['libelle_agence_demande_clean'] != '']
        print(f"   -> {initial_rows - len(df_demande)} lignes supprimées car libelle_agence_demande est vide.")
        if df_demande.empty: df_demande = None; print("❌ df_demande vide après filtrage libellé.")
        else: print(f"✅ df_demande prêt ({len(df_demande)} lignes).")
else: print("ℹ️ df_demande_raw non chargé ou vide.")


# =============================================================================
# 2. AGRÉGATION ET INSERTION DANS faits_canevas (Fonctionne, mais on le fait)
# =============================================================================
print("\n--- 2. Agrégation et Insertion dans faits_canevas ---")
faits_canevas_data = []
df_canevas_agg = None
if df_canevas is not None and not dim_agences_df.empty:
    print(f"   -> Jointure df_canevas ({len(df_canevas)}) avec dim_agence ({len(dim_agences_df)})...")
    df_canevas['code_agence_int'] = df_canevas['code_agence_int'].astype('Int64')
    dim_agences_df['code_agence'] = dim_agences_df['code_agence'].astype('Int64') # Assure le type
    # Utilise toujours code_agence pour canevas si ça fonctionnait
    df_canevas_merged = pd.merge(df_canevas, dim_agences_df[['id_agence', 'code_agence']], left_on='code_agence_int', right_on='code_agence', how='inner')
    rows_failed_lookup_canevas = len(df_canevas) - len(df_canevas_merged)
    print(f"   -> {len(df_canevas_merged)} lignes Canevas jointes ({rows_failed_lookup_canevas} perdues).")

    if not df_canevas_merged.empty:
        print("   -> Agrégation Canevas...")
        df_canevas_agg = df_canevas_merged.groupby('id_agence').agg(
             capacite_avg=('capacite_row', 'mean'), taux_endettement_avg=('taux_endettement_row', 'mean'), nombre_fiche=('id_agence', 'size')
        ).reset_index()
        print(f"   -> {len(df_canevas_agg)} agences agrégées pour Canevas.")
        default_id_temps = get_any_id("SELECT id_temps FROM dim_temps LIMIT 1") ; default_id_produit = get_any_id("SELECT id_produit FROM dim_produit LIMIT 1") 
        default_id_fiche_etude = get_any_id("SELECT id_fiche_etude FROM dim_fiche_d_etude LIMIT 1")  ; default_id_client = get_any_id("SELECT id_client FROM dim_client LIMIT 1") 

        if df_canevas_agg is not None and not df_canevas_agg.empty:
            getcontext().prec=12
            for index, row in df_canevas_agg.iterrows():
                 id_ag = row.get('id_agence'); cap_val = row.get('capacite_avg'); te_val = row.get('taux_endettement_avg'); nf_val = row.get('nombre_fiche')
                 if pd.notna(id_ag):
                     tx_acc = Decimal(random.uniform(80.0, 100.0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                     tup = (int(id_ag), default_id_produit, default_id_temps, default_id_fiche_etude, tx_acc, default_id_client, float(cap_val) if pd.notna(cap_val) else None, float(te_val) if pd.notna(te_val) else None, int(nf_val) if pd.notna(nf_val) else None)
                     faits_canevas_data.append(tup)
            if faits_canevas_data:
                 try:
                     cursor.execute("TRUNCATE TABLE faits_canevas RESTART IDENTITY;");
                     q_ins = """INSERT INTO faits_canevas (id_agence, id_produit, id_temps, id_fiche_etude, taux_acceptation, id_client, capacite, taux_endettement, nombre_fiche) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                     cursor.executemany(q_ins, faits_canevas_data); conn.commit(); print(f"✅ Inséré {len(faits_canevas_data)} faits_canevas.")
                 except Exception as e: conn.rollback(); print(f"❌ Erreur insertion faits_canevas: {e}")
    else: print("ℹ️ df_canevas_merged vide.")
else: print("ℹ️ Skip faits_canevas.")

# =============================================================================
# 3. AGRÉGATION ET INSERTION DANS faits_decision (JOINTURE LIBELLE)
# =============================================================================
print("\n--- 3. Agrégation et Insertion dans faits_decision (Jointure Libellé) ---")
faits_decision_data = []
rows_processed_agg_decision = 0
rows_failed_lookup_decision = 0
df_decision_agg = None

if df_demande is not None and not dim_agences_df.empty:
    print(f"   -> Jointure df_demande ({len(df_demande)}) avec dim_agence ({len(dim_agences_df)}) sur libellé nettoyé...")

    # **** JOINTURE SUR LIBELLE NETTOYÉ ****
    df_demande_merged = pd.merge(
        df_demande,
        dim_agences_df[['id_agence', 'libelle_agence_clean']], # Sélectionner les colonnes nécessaires de dim_agence
        left_on='libelle_agence_demande_clean',   # Clé nettoyée dans df_demande
        right_on='libelle_agence_clean',          # Clé nettoyée dans dim_agences_df
        how='inner'                               # Garder seulement les correspondances
    )
    # ***************************************

    rows_failed_lookup_decision = len(df_demande) - len(df_demande_merged) # Lignes perdues sur jointure libellé
    print(f"   -> {len(df_demande_merged)} lignes Demande jointes sur libellé ({rows_failed_lookup_decision} perdues).")

    # --- DEBUG: Afficher quelques libellés qui n'ont pas matché ---
    if rows_failed_lookup_decision > 0:
        libelles_demande_set = set(df_demande['libelle_agence_demande_clean'].unique())
        libelles_dim_set = set(dim_agences_df['libelle_agence_clean'].unique())
        libelles_non_trouves = list(libelles_demande_set - libelles_dim_set)
        if libelles_non_trouves:
            print(f"   -> !! DEBUG: {len(libelles_non_trouves)} libellés uniques présents dans Demande mais PAS dans dim_agence (échantillon): {libelles_non_trouves[:10]}...")
    # -----------------------------------------------------------

    if not df_demande_merged.empty:
        print("   -> Agrégation Decision...")
        delay_cols = ['duree_credit', 'delai_validation', 'delai_mise_en_place', 'delai_traitement']
        if not all(col in df_demande_merged.columns for col in delay_cols): print(f"   -> !! ERREUR: Cols délai manquantes Decision.")
        else:
             df_decision_agg = df_demande_merged.groupby('id_agence').agg(
                 duree_credit_avg=(delay_cols[0], 'mean'), delai_validation_avg=(delay_cols[1], 'mean'),
                 delai_mise_en_place_avg=(delay_cols[2], 'mean'), delai_traitement_avg=(delay_cols[3], 'mean'),
                 nombre_credit=('id_agence', 'size') # Compter par id_agence après jointure
             ).reset_index()
             print(f"   -> {len(df_decision_agg)} agences agrégées pour Decision.")

             # ... (Récup IDs défaut) ...
             default_id_produit_dec = get_any_id("SELECT id_produit FROM dim_produit LIMIT 1") 
             default_id_temps_dec = get_any_id("SELECT id_temps FROM dim_temps LIMIT 1") 
             default_id_decision_dec = get_any_id("SELECT id_decision FROM dim_sort_credit LIMIT 1") 
             default_id_client_dec = get_any_id("SELECT id_client FROM dim_client LIMIT 1") 

             if df_decision_agg is not None and not df_decision_agg.empty:
                 print(f"   -> Préparation des tuples faits_decision ({len(df_decision_agg)} lignes)...")
                 for index, row in df_decision_agg.iterrows():
                     id_ag = row.get('id_agence')
                     if pd.notna(id_ag):
                         nombre_credit_val = row.get('nombre_credit')
                         nombre_credit_final = None
                         if pd.notna(nombre_credit_val):
                             try:
                                 temp_int = int(nombre_credit_val)
                                 if temp_int >= 1: nombre_credit_final = temp_int
                             except (ValueError, TypeError): pass # Erreur silencieuse ici, on veut juste None si ça échoue

                         mesures_db = {
                             'duree_credit': int(round(row.get('duree_credit_avg'))) if pd.notna(row.get('duree_credit_avg')) else None,
                             'delai_validation': int(round(row.get('delai_validation_avg'))) if pd.notna(row.get('delai_validation_avg')) else None,
                             'delai_mise_en_place': int(round(row.get('delai_mise_en_place_avg'))) if pd.notna(row.get('delai_mise_en_place_avg')) else None,
                             'delai_traitement': int(round(row.get('delai_traitement_avg'))) if pd.notna(row.get('delai_traitement_avg')) else None,
                             'nombre_credit': nombre_credit_final
                         }
                         data_tuple_dec = (
                             int(id_ag), default_id_produit_dec, default_id_temps_dec, default_id_decision_dec,
                             mesures_db.get('duree_credit'), mesures_db.get('delai_validation'), mesures_db.get('delai_mise_en_place'), mesures_db.get('delai_traitement'),
                             default_id_client_dec, mesures_db.get('nombre_credit')
                         )
                         faits_decision_data.append(data_tuple_dec)

                 if faits_decision_data:
                      print(f"⏳ Insertion faits_decision...");
                      try:
                          cursor.execute("TRUNCATE TABLE faits_decision RESTART IDENTITY;")
                          insert_query_decision = """INSERT INTO faits_decision (id_agence, id_produit, id_temps, id_decision, duree_credit, delai_validation, delai_mise_en_place, delai_traitement, id_client, nombre_credit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                          cursor.executemany(insert_query_decision, faits_decision_data)
                          conn.commit(); print(f"✅ Inséré {len(faits_decision_data)} lignes faits_decision.")
                      except Exception as e: conn.rollback(); print(f"❌ Erreur insertion faits_decision : {e}"); print(f"   -> Données ex: {faits_decision_data[0] if faits_decision_data else 'N/A'}")
                 else: print("ℹ️ Aucune donnée Decision à insérer.")
    else: print("ℹ️ df_demande_merged vide après jointure sur libellé.")
else: print("ℹ️ Skip faits_decision (df_demande ou dim_agence invalide/vide).")


# =============================================================================
# 4. VÉRIFICATION Post-Insertion
# =============================================================================
print("\n--- 4. Vérification Post-Insertion (Exemple) ---")
# ... (code de vérification inchangé) ...
check_id_agence_canevas = faits_canevas_data[0][0] if faits_canevas_data else None
check_id_agence_decision = faits_decision_data[0][0] if faits_decision_data else None

if check_id_agence_canevas is not None: # Vérification Canevas (devrait rester OK)
    try:
        cursor.execute("SELECT nombre_fiche, capacite, taux_endettement FROM faits_canevas WHERE id_agence = %s", (int(check_id_agence_canevas),))
        result = cursor.fetchone(); nf, cap, te = result if result else (None, None, None)
        print(f"   -> Vérif Canevas (agence {check_id_agence_canevas}): Nombre Fiches={nf}, Capacité={cap}, Taux Endettement={te}")
    except Exception as e: print(f"   -> Erreur vérification Canevas : {e}")
else: print("   -> Skip vérif faits_canevas.")

if check_id_agence_decision is not None: # Vérification Decision (le point clé)
    try:
        cursor.execute("SELECT nombre_credit FROM faits_decision WHERE id_agence = %s", (int(check_id_agence_decision),))
        result = cursor.fetchone(); nc = result[0] if result else None
        print(f"   -> Vérif Decision (agence {check_id_agence_decision}): Nombre Crédits LU = {nc}")
        if nc is None or (isinstance(nc, int) and nc <= 0):
            print(f"      -> !! ÉCHEC POSSIBLE: nombre_credit est {nc} (NULL ou invalide) !!")
        else:
             print(f"      -> ✅ SUCCÈS POSSIBLE: nombre_credit ({nc}) lu correctement.")
    except Exception as e: print(f"   -> Erreur vérification Decision : {e}")
else: print("   -> Skip vérif faits_decision.")


# =============================================================================
# 5. FINALISATION
# =============================================================================
print("\n--- 5. Finalisation ---")
print(f"Stats Canevas: {len(faits_canevas_data)} lignes insérées.")
print(f"Stats Decision: {len(faits_decision_data)} lignes insérées.")
if cursor: cursor.close()
if conn: conn.close()
print("🔌 Connexion fermée.")
print(f"✅ Processus ETL terminé ({datetime.now()}).")



#ETL_temps
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
    cursor = conn.cursor()
    cursor.execute("SET search_path TO public;")
    print("\n✅ Connexion réussie à la base de données PostgreSQL.")
except psycopg2.OperationalError as e:
    print(f"\n❌ Erreur de connexion : {e}")
    exit()

# Charger le fichier CANEVAS_RETAILS.csv
df_canevas = pd.read_csv("C:/Users/feres/Downloads/CANEVAS_RETAILS.csv", dtype=str)

# Trouver la ligne des en-têtes
header_row_canevas = df_canevas[df_canevas.astype(str).apply(lambda x: x.str.contains("CANEVAS", na=False)).any(axis=1)].index[0]
df_canevas.columns = df_canevas.iloc[header_row_canevas]
df_canevas = df_canevas[(header_row_canevas + 1):].reset_index(drop=True)

# Extraire la colonne 'Date MEP'
df_canevas_date_mep = df_canevas[['Date MEP']].copy()

# Charger le fichier DEMANDE_CG_2018_2019_2020-_1_.csv
df_demande = pd.read_csv("C:/Users/feres/Downloads/DEMANDE_CG_2018_2019_2020-_1_.csv", dtype=str)

# Colonnes de dates dans DEMANDE_CG
colonnes_dates_demande = ["DAT_DEM_DECCG", "DAT_VALDR_DECCG", "DAT_VALCCI_DECCG", "DAT_MEP_DECCG"]
df_demande_dates = df_demande[colonnes_dates_demande].copy()

# Fusionner les deux DataFrames de dates
all_dates = pd.concat([df_canevas_date_mep, df_demande_dates], ignore_index=True)

# Convertir les dates de type chaîne en datetime
all_dates['date_complete'] = pd.to_datetime(all_dates['Date MEP'], format='%d/%m/%Y', errors='coerce')

# Supprimer les dates invalides (NaT)
all_dates = all_dates.dropna(subset=['date_complete']).reset_index(drop=True)

# Extraire les informations temporelles
all_dates['jour'] = all_dates['date_complete'].dt.day
all_dates['mois'] = all_dates['date_complete'].dt.month
all_dates['annee'] = all_dates['date_complete'].dt.year
all_dates['trimestre'] = all_dates['date_complete'].dt.quarter

# Créer un ID unique pour chaque date
all_dates['id_temps'] = range(1, len(all_dates) + 1)

# Connexion à PostgreSQL pour insérer les données dans la table dim_temps
for _, row in all_dates.iterrows():
    query = """
        INSERT INTO dim_temps (id_temps, date, jour, mois, annee, trimestre)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        row['id_temps'], 
        row['date_complete'].strftime('%Y-%m-%d'),  # Formater la date pour l'insertion
        row['jour'], 
        row['mois'], 
        row['annee'], 
        row['trimestre']
    ))

# Commit des modifications et fermer la connexion
conn.commit()
print("\n✅ Table dim_temps remplie avec succès !")

# Fermer la connexion
cursor.close()
conn.close()


df_canevas_raw = charger_donnees("CANEVAS_RETAILS.csv", dtype=str, skip_initial_rows=3)
df_demande_raw = charger_donnees("DEMANDE_CG_2018_2019_2020-_1_.csv", dtype=str, skip_initial_rows=0)

cursor.execute("SELECT id_agence, code_agence, libelle_agence FROM dim_agence")
dim_agences_df = pd.DataFrame(cursor.fetchall(), columns=['id_agence', 'code_agence', 'libelle_agence'])

df_canevas['Revenus_Annuel_num'] = df_canevas['Revenus_Annuel'].apply(clean_numeric)
df_canevas['Retenus_Mensuel_num'] = df_canevas['Retenus_Mensuel'].apply(clean_numeric)
df_canevas['Montant_Sollicité_num'] = df_canevas['Montant_Sollicité'].apply(clean_numeric)








try:
     conn = psycopg2.connect(
          dbname="datawarehauseBNA", user="postgres", password="1234", 
         host="localhost", port="5435", client_encoding='UTF8'
     )
     cursor = conn.cursor()
     print("Connexion à PostgreSQL réussie.")
except Exception as e: print(f" Erreur connexion: {e}"); exit()
df_canevas_raw = charger_donnees("...CANEVAS_RETAILS.csv", ...)
df_demande_raw = charger_donnees("...DEMANDE_CG_2018_2019_2020.csv", ...)

cursor.execute("SELECT id_agence, code_agence, libelle_agence FROM dim_agence")
dim_agences_df = pd.DataFrame(cursor.fetchall(), columns=['id_agence', 'code_agence', 'libelle_agence'])

















df_canevas['code_agence_int'] = pd.to_numeric(df_canevas['Agence_Initiatrice'], errors='coerce').astype('Int64')


df_canevas['revenu_annuel_num'] = df_canevas['Revenus_Annuel'].apply(clean_numeric)
df_canevas['retenues_mensuel_num'] = df_canevas['Retenus_Mensuel'].apply(clean_numeric)
df_canevas['montant_sollicite_num'] = df_canevas['Montant_Sollicité'].apply(clean_numeric)

df_canevas['capacite_remboursement'] = (
    (df_canevas['revenu_annuel_num'] - df_canevas['retenues_mensuel_num'] * 12) * 0.40
)


df_canevas['taux_endettement'] = np.where(
    (df_canevas['revenu_annuel_num'].notna()) & (df_canevas['revenu_annuel_num'] > 0),
    df_canevas['montant_sollicite_num'] / df_canevas['revenu_annuel_num'],
    np.nan
)
df_canevas['taux_endettement'].replace([np.inf, -np.inf], np.nan, inplace=True)


df_canevas = df_canevas.dropna(subset=['code_agence_int'])






import pandas as pd
import numpy as np

# Fonction de nettoyage des montants (revenus, retenues, montants sollicités)
def clean_numeric(val):
    try:
        if isinstance(val, str):
            val = val.replace(" ", "").replace(",", ".")
        return float(val)
    except:
        return np.nan


df_canevas['code_agence_int'] = pd.to_numeric(df_canevas['Agence_Initiatrice'], errors='coerce').astype('Int64')


df_canevas['revenu_annuel_num'] = df_canevas['Revenus_Annuel'].apply(clean_numeric)
df_canevas['retenues_mensuel_num'] = df_canevas['Retenus_Mensuel'].apply(clean_numeric)
df_canevas['montant_sollicite_num'] = df_canevas['Montant_Sollicité'].apply(clean_numeric)


df_canevas['capacite_remboursement'] = (
    (df_canevas['revenu_annuel_num'] - df_canevas['retenues_mensuel_num'] * 12) * 0.40
)


df_canevas['taux_endettement'] = np.where(
    (df_canevas['revenu_annuel_num'].notna()) & (df_canevas['revenu_annuel_num'] > 0),
    df_canevas['montant_sollicite_num'] / df_canevas['revenu_annuel_num'],
    np.nan
)
df_canevas['taux_endettement'].replace([np.inf, -np.inf], np.nan, inplace=True)


df_canevas = df_canevas.dropna(subset=['code_agence_int'])







for index, row in df_canevas_agg.iterrows():
    faits_canevas_data.append((...))


cursor.execute("TRUNCATE TABLE faits_canevas RESTART IDENTITY;")
cursor.executemany("""
    INSERT INTO faits_canevas (id_agence, id_produit, id_temps, id_fiche_etude, taux_acceptation, id_client, capacite_moyenne, taux_endettement_moyen, nombre_fiches)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", faits_canevas_data)
conn.commit()





import pandas as pd


file_path = "C:/Users/feres/Downloads/CANEVAS_RETAILS.csv"
df = pd.read_csv(file_path, sep=",", header=None, encoding="utf-8", dtype=str)


header_row = df[df.astype(str).apply(lambda x: x.str.contains("CANEVAS", na=False)).any(axis=1)].index[0]


df.columns = df.iloc[header_row]
df = df[(header_row + 1):].reset_index(drop=True)

print("Extraction terminée")







from datetime import datetime
# Sélection des colonnes nécessaires
df_client = df[['N°CANEVAS', 'Profession', 'Date Naissance']].copy()

# Renommage des colonnes 
df_client.rename(columns={
    'N°CANEVAS': 'id_client',
    'Profession': 'profession',
    'Date Naissance': 'date_naissance'
}, inplace=True)


df_client['id_client'] = df_client['id_client'].str.strip()
df_client['profession'] = df_client['profession'].str.strip().str.title()


def calculate_age(date_str):
    try:
        birth_date = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        today = datetime.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        if age < 0 or age > 150:
            return None
        return age
    except:
        return None

df_client['age'] = df_client['date_naissance'].apply(calculate_age)

df_client.dropna(subset=['id_client', 'profession', 'age'], inplace=True)

df_client.drop(columns=['date_naissance'], inplace=True)
print("Transformation et nettoyage terminés")
print(df_client.head())







try:
    conn = psycopg2.connect(
        dbname="datawarehauseBNA",
        user="postgres",
        password="1234",
        host="localhost",
        port="5435",
        client_encoding='UTF8'
    )
    print("Connexion PostgreSQL établie")
except psycopg2.OperationalError as e:
    print(f"Erreur de connexion : {e}")
    exit()

cur = conn.cursor()

# Insertion 
for index, row in df_client.iterrows():
    cur.execute("""
        INSERT INTO dim_client (id_client, profession, age)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_client)
        DO UPDATE SET profession = EXCLUDED.profession, age = EXCLUDED.age
    """, (row['id_client'], row['profession'], row['age']))

conn.commit()
cur.close()
conn.close()

print("Chargement dans dim_client terminé avec succès")
