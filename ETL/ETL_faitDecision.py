import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import io
from decimal import Decimal, getcontext, ROUND_HALF_UP
import random
import re
print(f"Début du script ETL Faits (Agrégation Finale v11.6 - Jointure Libellé) ({datetime.now()})")

try:
     conn = psycopg2.connect(
          dbname="datawarehauseBNA", user="postgres", password="1234", 
         host="localhost", port="5435", client_encoding='UTF8'
     )
     cursor = conn.cursor()
     print(" Connexion à PostgreSQL réussie.")
except Exception as e: print(f" Erreur connexion: {e}"); exit()
placeholder = 'INCONNU'

def clean_numeric(value): 
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
    except FileNotFoundError: print(f"Non trouvé - {chemin}"); return None
    except Exception as e: print(f" Erreur chargement {chemin} : {e}"); return None
def convertir_date(date_str): 
    if pd.isna(date_str) or date_str == '': return pd.NaT
    try: return pd.to_datetime(date_str, format='%d/%m/%y', errors='raise')
    except ValueError:
        try: return pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce')
        except Exception: return pd.NaT
def get_any_id(query): 
    try: cursor.execute(query); result = cursor.fetchone(); return result[0] if result else None
    except Exception as e: print(f" Erreur get_any_id query='{query}': {e}"); conn.rollback(); return None
print("\nChargement et Préparation des Données ")
df_canevas_raw = charger_donnees("C:/Users/feres/Downloads/CANEVAS_RETAILS.csv", dtype=str, header_keyword='CANEVAS', skip_initial_rows=3) # Charger même si on saute section 2 plus tard
df_demande_raw = charger_donnees("C:/Users/feres/Downloads/DEMANDE_CG_2018_2019_2020-_1_.csv", dtype=str, header_keyword='LIB_AGENCE', skip_initial_rows=0)

dim_agences_df = pd.DataFrame()
print("⚙️  Chargement dim_agence (avec libelle)...")
try:
    cursor.execute("SELECT id_agence, code_agence, libelle_agence FROM dim_agence")
    dim_agences_list = cursor.fetchall()
    dim_agences_df = pd.DataFrame(dim_agences_list, columns=['id_agence', 'code_agence', 'libelle_agence'])

    dim_agences_df['libelle_agence_clean'] = dim_agences_df['libelle_agence'].fillna('').astype(str).str.strip().str.upper()

    dim_agences_df['code_agence'] = pd.to_numeric(dim_agences_df['code_agence'], errors='coerce').astype('Int64')


    initial_dim_rows = len(dim_agences_df)
    dim_agences_df.dropna(subset=['id_agence'], inplace=True) 
    dim_agences_df = dim_agences_df[dim_agences_df['libelle_agence_clean'] != '']
    print(f"   -> {initial_dim_rows} agences lues, {len(dim_agences_df)} conservées après nettoyage libellé.")

    if dim_agences_df.duplicated(subset=['libelle_agence_clean']).any():
        print(f"   -> !! ATTENTION: {dim_agences_df.duplicated(subset=['libelle_agence_clean']).sum()} libellés d'agence DUPLIQUÉS après nettoyage dans dim_agence !")
        print("      Exemples de doublons:", dim_agences_df[dim_agences_df.duplicated(subset=['libelle_agence_clean'], keep=False)]['libelle_agence_clean'].unique()[:5])
except Exception as e: print(f"Erreur chargement dim_agence : {e}")

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
    else: df_canevas = None; print("Cols manquantes df_canevas.")
df_demande = None
col_lib_agence_demande = 'LIB_AGENCE' 
if df_demande_raw is not None and not df_demande_raw.empty:
    print(f"⚙️  Préparation df_demande (focus libellé agence)...")
    df_demande = df_demande_raw.copy()
    required_cols_dem = ['DAT_DEM_DECCG', 'DAT_VALDR_DECCG', 'DAT_VALCCI_DECCG', 'DAT_MEP_DECCG', col_lib_agence_demande]
    if not all(col in df_demande.columns for col in required_cols_dem):
         print(f"❌ Cols manquantes df_demande : {[col for col in required_cols_dem if col not in df_demande.columns]}.")
         df_demande = None
    else:
        df_demande['libelle_agence_demande_clean'] = df_demande[col_lib_agence_demande].fillna('').astype(str).str.strip().str.upper()

        date_cols_demande = ['DAT_DEM_DECCG', 'DAT_VALDR_DECCG', 'DAT_VALCCI_DECCG', 'DAT_MEP_DECCG']
        for col in date_cols_demande: df_demande[col] = df_demande[col].apply(convertir_date)
        date_pairs = [('duree_credit', 'DAT_MEP_DECCG', 'DAT_DEM_DECCG'), ('delai_validation', 'DAT_VALDR_DECCG', 'DAT_DEM_DECCG'), ('delai_mise_en_place', 'DAT_MEP_DECCG', 'DAT_VALCCI_DECCG'), ('delai_traitement', 'DAT_MEP_DECCG', 'DAT_DEM_DECCG')]
        for m, e, s in date_pairs:
             if e in df_demande.columns and s in df_demande.columns:
                 mask = df_demande[s].notna() & df_demande[e].notna(); df_demande[m] = pd.NA
                 df_demande.loc[mask, m] = (df_demande.loc[mask, e] - df_demande.loc[mask, s]).dt.days
                 df_demande[m] = pd.to_numeric(df_demande[m], errors='coerce').astype('Int64')
             else: df_demande[m] = pd.NA

        initial_rows = len(df_demande)
        df_demande = df_demande[df_demande['libelle_agence_demande_clean'] != '']
        print(f"   -> {initial_rows - len(df_demande)} lignes supprimées car libelle_agence_demande est vide.")
        if df_demande.empty: df_demande = None; print("df_demande vide après filtrage libellé.")
        else: print(f" df_demande prêt ({len(df_demande)} lignes).")
else: print("df_demande_raw non chargé ou vide.")

print("\n--- 2. Agrégation et Insertion dans faits_canevas ---")
faits_canevas_data = []
df_canevas_agg = None
if df_canevas is not None and not dim_agences_df.empty:
    print(f"   -> Jointure df_canevas ({len(df_canevas)}) avec dim_agence ({len(dim_agences_df)})...")
    df_canevas['code_agence_int'] = df_canevas['code_agence_int'].astype('Int64')
    dim_agences_df['code_agence'] = dim_agences_df['code_agence'].astype('Int64')

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
                     cursor.executemany(q_ins, faits_canevas_data); conn.commit(); print(f"Inséré {len(faits_canevas_data)} faits_canevas.")
                 except Exception as e: conn.rollback(); print(f" Erreur insertion faits_canevas: {e}")
    else: print(" df_canevas_merged vide.")
else: print("Skip faits_canevas.")

print("\n--- 3. Agrégation et Insertion dans faits_decision (Jointure Libellé) ---")
faits_decision_data = []
rows_processed_agg_decision = 0
rows_failed_lookup_decision = 0
df_decision_agg = None

if df_demande is not None and not dim_agences_df.empty:
    print(f"   -> Jointure df_demande ({len(df_demande)}) avec dim_agence ({len(dim_agences_df)}) sur libellé nettoyé...")

    df_demande_merged = pd.merge(
        df_demande,
        dim_agences_df[['id_agence', 'libelle_agence_clean']], 
        left_on='libelle_agence_demande_clean',  
        right_on='libelle_agence_clean',          
        how='inner'                               
    )
    rows_failed_lookup_decision = len(df_demande) - len(df_demande_merged) 
    print(f"   -> {len(df_demande_merged)} lignes Demande jointes sur libellé ({rows_failed_lookup_decision} perdues).")

    if rows_failed_lookup_decision > 0:
        libelles_demande_set = set(df_demande['libelle_agence_demande_clean'].unique())
        libelles_dim_set = set(dim_agences_df['libelle_agence_clean'].unique())
        libelles_non_trouves = list(libelles_demande_set - libelles_dim_set)
        if libelles_non_trouves:
            print(f"   -> !! DEBUG: {len(libelles_non_trouves)} libellés uniques présents dans Demande mais PAS dans dim_agence (échantillon): {libelles_non_trouves[:10]}...")

    if not df_demande_merged.empty:
        print("   -> Agrégation Decision...")
        delay_cols = ['duree_credit', 'delai_validation', 'delai_mise_en_place', 'delai_traitement']
        if not all(col in df_demande_merged.columns for col in delay_cols): print(f"   -> !! ERREUR: Cols délai manquantes Decision.")
        else:
             df_decision_agg = df_demande_merged.groupby('id_agence').agg(
                 duree_credit_avg=(delay_cols[0], 'mean'), delai_validation_avg=(delay_cols[1], 'mean'),
                 delai_mise_en_place_avg=(delay_cols[2], 'mean'), delai_traitement_avg=(delay_cols[3], 'mean'),
                 nombre_credit=('id_agence', 'size') 
             ).reset_index()
             print(f"   -> {len(df_decision_agg)} agences agrégées pour Decision.")

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
                             except (ValueError, TypeError): pass 

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
                          conn.commit(); print(f" Inséré {len(faits_decision_data)} lignes faits_decision.")
                      except Exception as e: conn.rollback(); print(f" Erreur insertion faits_decision : {e}"); print(f"   -> Données ex: {faits_decision_data[0] if faits_decision_data else 'N/A'}")
                 else: print("ℹ️ Aucune donnée Decision à insérer.")
    else: print("ℹ️ df_demande_merged vide après jointure sur libellé.")
else: print("ℹ️ Skip faits_decision (df_demande ou dim_agence invalide/vide).")

print("\n--- 4. Vérification Post-Insertion (Exemple) ---")
check_id_agence_canevas = faits_canevas_data[0][0] if faits_canevas_data else None
check_id_agence_decision = faits_decision_data[0][0] if faits_decision_data else None

if check_id_agence_canevas is not None: 
    try:
        cursor.execute("SELECT nombre_fiche, capacite, taux_endettement FROM faits_canevas WHERE id_agence = %s", (int(check_id_agence_canevas),))
        result = cursor.fetchone(); nf, cap, te = result if result else (None, None, None)
        print(f"   -> Vérif Canevas (agence {check_id_agence_canevas}): Nombre Fiches={nf}, Capacité={cap}, Taux Endettement={te}")
    except Exception as e: print(f"   -> Erreur vérification Canevas : {e}")
else: print("   -> Skip vérif faits_canevas.")

if check_id_agence_decision is not None: 
    try:
        cursor.execute("SELECT nombre_credit FROM faits_decision WHERE id_agence = %s", (int(check_id_agence_decision),))
        result = cursor.fetchone(); nc = result[0] if result else None
        print(f"   Vérif Decision (agence {check_id_agence_decision}): Nombre Crédits LU = {nc}")
        if nc is None or (isinstance(nc, int) and nc <= 0):
            print(f"  nombre_credit est {nc} (NULL ou invalide) !!")
        else:
             print(f" nombre_credit ({nc}) lu correctement.")
    except Exception as e: print(f"   -> Erreur vérification Decision : {e}")
else: print("   -> Skip vérif faits_decision.")

print("\n--- 5. Finalisation ---")
print(f"Stats Canevas: {len(faits_canevas_data)} lignes insérées.")
print(f"Stats Decision: {len(faits_decision_data)} lignes insérées.")
if cursor: cursor.close()
if conn: conn.close()
print("🔌 Connexion fermée.")
print(f"Processus ETL terminé ({datetime.now()}).")







df_decision = pd.read_csv('DEMANDE_CG_2018_2019_2020-1.csv', encoding='utf-8', sep=';', dtype=str)

cursor.execute("SELECT id_agence, code_agence, libelle_agence FROM dim_agence")
agences_data = cursor.fetchall()
dim_agences_df = pd.DataFrame(agences_data, columns=['id_agence', 'code_agence', 'libelle_agence'])





df_decision['code_agence'] = df_decision['code_agence'].astype(str).str.strip()
df_decision['libelle_agence'] = df_decision['libelle_agence'].astype(str).str.upper().str.strip()
dim_agences_df['libelle_agence_clean'] = dim_agences_df['libelle_agence'].astype(str).str.upper().str.strip()

df_decision['Date_decision'] = pd.to_datetime(df_decision['Date_decision'], errors='coerce')
df_decision['Date_Arrivée'] = pd.to_datetime(df_decision['Date_Arrivée'], errors='coerce')

df_decision['delai_traitement'] = (df_decision['Date_decision'] - df_decision['Date_Arrivée']).dt.days

df_merged_decision = pd.merge(df_decision, dim_agences_df, on='code_agence', how='inner')


df_faits_decision = df_merged_decision.groupby('id_agence').agg(
    nombre_demandes=('N°DEMANDE', 'count'),
    delai_moyen=('delai_traitement', 'mean')
).reset_index()





insert_query_decision = """
    INSERT INTO faits_decision (
        id_agence, id_temps, id_client, id_produit, id_fiche_etude,
        nombre_demandes, delai_traitement_moyen
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(insert_query_decision, faits_decision_data)
conn.commit()
print(f"{len(faits_decision_data)} lignes insérées dans faits_decision.")