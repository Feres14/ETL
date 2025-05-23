import psycopg2
import pandas as pd
from datetime import datetime


file_path = "C:/Users/feres/Downloads/CANEVAS_RETAILS.csv"
df = pd.read_csv(file_path, sep=",", header=None, encoding="utf-8", dtype=str)

header_row = df[df.astype(str).apply(lambda x: x.str.contains("CANEVAS", na=False)).any(axis=1)].index[0]

df.columns = df.iloc[header_row]
df = df[(header_row + 1):].reset_index(drop=True)

print(" Données avant transformation :")
print(df.head())

df_client = df[['N°CANEVAS', 'Profession', 'Date Naissance']].copy()
df_client.rename(columns={'N°CANEVAS': 'id_client', 'Profession': 'profession'}, inplace=True)

def calculate_age(date_str):
    try:
        birth_year = int(date_str.split("/")[-1])  
        current_year = datetime.now().year
        age = current_year - birth_year
        if age > 150:
            return None
        return age
    except:
        return None

df_client['age'] = df_client['Date Naissance'].apply(calculate_age)
df_client.drop(columns=['Date Naissance'], inplace=True)


df_client = df_client[df_client['age'] <= 150]

print("Données après transformation :")
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
    print(" Connexion réussie à la base de données PostgreSQL.")
except psycopg2.OperationalError as e:
    print(" Erreur de connexion : {e}")
    exit()

cur = conn.cursor()

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

print(" Données insérées dans dim_client avec succès ") 