import pandas as pd

# Chemin du fichier source
file_path = "C:/Users/feres/Downloads/CANEVAS_RETAILS.csv"

# Charger le fichier sans header, en traitant toutes les lignes comme données
df = pd.read_csv(file_path, header=None, encoding="utf-8", dtype=str)

# Remplacer les chaînes vides par NaN et supprimer les lignes entièrement vides
df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
df = df.dropna(how='all')

# Chercher la première ligne contenant le mot "CANEVAS" dans l'une des colonnes (insensible à la casse)
header_index = None
for i, row in df.iterrows():
    if row.astype(str).str.contains("CANEVAS", case=False, na=False).any():
        header_index = i
        break

if header_index is None:
    raise ValueError("Aucune ligne d'en-tête n'a été trouvée contenant 'CANEVAS'.")

# Définir cette ligne comme en-tête
df.columns = df.iloc[header_index].str.strip()  # Nettoyer les noms de colonnes
# Conserver uniquement les lignes à partir de celle qui suit l'en-tête
df_clean = df.iloc[header_index+1:].reset_index(drop=True)

# Sauvegarder le fichier nettoyé
output_file = "CANEVAS_RETAILS_clean.csv"
df_clean.to_csv(output_file, index=False, encoding="utf-8")
print(f"Fichier nettoyé enregistré sous '{output_file}'")