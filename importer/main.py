import os
import time
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, time as dt_time

sys.stdout.reconfigure(line_buffering=True)

# --- CONFIGURATION (Inchangée) ---

DB_USER = os.getenv('MYSQL_USER')
DB_PASSWORD = os.getenv('MYSQL_PASSWORD')
DB_HOST = os.getenv('DB_HOST', 'kpi_mariadb')
DB_NAME = os.getenv('MYSQL_DATABASE')
DATA_FOLDER = '/data'

FILES_MAPPING = {
    "ShopActivityRecent.csv": "ShopActivityRecent",
    "Build_MPS_Pulse_MLG.csv": "build_mlg_raw",
    "Build_MPS_Pulse_NLG.csv": "build_nlg_raw",
    "Calendrier.csv": "calendrier_raw",
    "ShopActivityHistorical.csv": "ShopActivityHistorical",
    "Reference_WorkCenter.csv": "Reference_WorkCenter",
    "Reference_Employee.csv": "Reference_Employee",
    "Reference_DIPlan.csv": "Reference_DIPlan",
    "Reference_Department.csv": "Reference_Department",
    "OrderOperation.csv": "OrderOperation",
    "OrderHeader.csv": "OrderHeader",
    "DIActivity.csv": "DIActivity",
    "Config_Stations.csv": "config_stations_raw"
}

# --- TES FONCTIONS DE CALCUL (Extraites pour être utilisables) ---

def calculer_heures_hebdomadaires(df_cal):
    heures_hebdo = df_cal.groupby(['Année', 'Semaine'])['Ouverture'].sum().reset_index()
    return heures_hebdo.rename(columns={'Ouverture': 'Total_Heures_Semaine'})

def calculer_production_par_semaine(df_build):
    df_clean = df_build.dropna(subset=['Année', 'Semaine_Num', 'OF']).copy()
    production = df_clean.groupby(['Année', 'Semaine_Num'])['OF'].count().reset_index()
    production.columns = ['Année', 'Semaine', 'Nb_Pieces_A_Produire']
    return production.sort_values(by=['Année', 'Semaine'])

def calculer_takt_time(df_heures, df_pieces):
    takt_df = pd.merge(df_heures, df_pieces, on=['Année', 'Semaine'], how='left')
    takt_df['Nb_Pieces_A_Produire'] = takt_df['Nb_Pieces_A_Produire'].fillna(0)
    takt_df['Takt'] = takt_df.apply(lambda r: r['Total_Heures_Semaine'] / r['Nb_Pieces_A_Produire'] if r['Nb_Pieces_A_Produire'] > 0 else 0, axis=1)
    return takt_df[['Année', 'Semaine', 'Takt']]

def generer_planning_interruptions(df_cal, num_annee, num_semaine, takt_hrs):
    semaine_data = df_cal[(df_cal['Année'] == num_annee) & (df_cal['Semaine'] == num_semaine) & (df_cal['Ouverture'] > 0)].copy().sort_values(by='Date')
    if semaine_data.empty: return pd.DataFrame()
    segments = []
    
    for _, row in semaine_data.iterrows():
        # --- LA CORRECTION EST ICI ---
        # On force la transformation du texte ("08:00") en véritable format Heure
        h_deb = pd.to_datetime(str(row['Début'])).time()
        h_fin = pd.to_datetime(str(row['Fin'])).time()
        
        debut = datetime.combine(row['Date'], h_deb)
        fin = datetime.combine(row['Date'], h_fin)
        if fin <= debut: fin += timedelta(days=1)
        segments.append({'debut': debut, 'fin': fin})
        
    planning = []
    takt_delta = timedelta(hours=takt_hrs)
    idx_seg, curseur = 0, segments[0]['debut']
    num_takt = 1
    
    while idx_seg < len(segments):
        deb_reel, reste, interrompu = curseur, takt_delta, "Non"
        while reste > timedelta(0) and idx_seg < len(segments):
            dispo = segments[idx_seg]['fin'] - curseur
            if reste <= dispo:
                h_fin_takt = curseur + reste
                planning.append({'Takt': f"Pièce {num_takt}", 'Heure Début': deb_reel.strftime("%d/%m %H:%M"), 'Heure Fin': h_fin_takt.strftime("%d/%m %H:%M"), 'Interrompu': interrompu})
                num_takt += 1
                curseur = h_fin_takt
                reste = timedelta(0)
                if curseur >= segments[idx_seg]['fin']: idx_seg += 1
            else:
                reste -= dispo
                idx_seg += 1
                interrompu = "Oui"
                if idx_seg < len(segments): curseur = segments[idx_seg]['debut']
    return pd.DataFrame(planning)

def obtenir_liste_of_etendue(df_build, num_annee, num_semaine):
    an_suiv, sem_suiv = (num_annee + 1, 1) if num_semaine >= 52 else (num_annee, num_semaine + 1)
    of_act = df_build[(df_build['Année'] == num_annee) & (df_build['Semaine_Num'] == num_semaine)].sort_values(by='Séquence')
    of_sui = df_build[(df_build['Année'] == an_suiv) & (df_build['Semaine_Num'] == sem_suiv)].sort_values(by='Séquence')
    return pd.concat([of_act, of_sui])['OF'].astype(str).tolist()

def attribuer_vrais_of_aux_postes(df_planning, liste_of_reels, df_config_ligne):
    df_c = df_planning.copy()
    if df_c.empty: return df_c
    
    # On utilise les noms de colonnes sécurisés en majuscules
    df_config_ligne = df_config_ligne.sort_values(by='STATION_NUM').reset_index(drop=True)
    nb_postes = len(df_config_ligne)
    
    for i in range(nb_postes - 1, -1, -1):
        nom_colonne = str(df_config_ligne.loc[i, 'OPERATION'])
        offset = (nb_postes - 1) - i
        
        df_c[nom_colonne] = df_c.apply(
            lambda row: (liste_of_reels[(int(row['Takt'].split()[-1]) - 1) + offset] 
                         if 0 <= (int(row['Takt'].split()[-1]) - 1) + offset < len(liste_of_reels) 
                         else "Libre"), axis=1)
    return df_c


def generer_planning_ligne(engine, ligne, nom_table_source, nom_table_destination, cal, df_config, semaine, annee):
    print(f"Lancement des calculs Takt pour la ligne {ligne}...", flush=True)
    
    # On filtre avec la colonne LIGNE sécurisée en majuscules
    config_ligne = df_config[df_config['LIGNE'] == ligne]
    
    if config_ligne.empty:
        print(f"Erreur : Aucune configuration de station trouvee pour {ligne}.", flush=True)
        return

    try:
        build = pd.read_sql(f"SELECT * FROM {nom_table_source}", engine)
    except Exception as e:
        print(f"Erreur lors de la lecture de {nom_table_source} : {e}", flush=True)
        return

    build['Date_Temp'] = pd.to_datetime(build['Date DDO'].astype(str).str.strip(), dayfirst=True, errors='coerce')
    build['Année'] = build['Date_Temp'].dt.year
    build['Semaine_Num'] = build['Sem DDO'].astype(str).str.extract(r'(\d+)').astype(float)

    res_h = calculer_heures_hebdomadaires(cal)
    res_p = calculer_production_par_semaine(build)
    res_t = calculer_takt_time(res_h, res_p)
    
    valeurs_takt = res_t[(res_t['Année'] == annee) & (res_t['Semaine'] == semaine)]['Takt']
    
    if valeurs_takt.empty or valeurs_takt.iloc[0] == 0:
        print(f"Attention : Takt Time de 0 pour {ligne} en S{semaine}-{annee}. Annulation.", flush=True)
        return 

    val_takt = valeurs_takt.iloc[0]
    print(f"Takt calcule pour {ligne} : {val_takt} heures/piece", flush=True)
    
    df_plan = generer_planning_interruptions(cal, annee, semaine, val_takt)
    liste_of = obtenir_liste_of_etendue(build, annee, semaine)
    
    planning_final = attribuer_vrais_of_aux_postes(df_plan, liste_of, config_ligne)

    if not planning_final.empty:
        planning_final['Ligne'] = ligne
        planning_final.to_sql(nom_table_destination, engine, if_exists='replace', index=False)
        print(f"Planning {ligne} envoye a MariaDB (table: {nom_table_destination}).", flush=True)


def executer_mes_calculs(engine):
    print("Debut de l'execution des calculs de planning...", flush=True)
    
    semaine = 5
    annee = 2026

    cal = pd.read_sql("SELECT * FROM calendrier_raw", engine)
    cal['Date'] = pd.to_datetime(cal['Date'], dayfirst=True)
    cal['Année'] = cal['Date'].dt.year
    cal['Semaine'] = cal['Date'].dt.isocalendar().week

    try:
        config_stations = pd.read_sql("SELECT * FROM config_stations_raw", engine)
        
        # --- NETTOYAGE ABSOLU DE LA CONFIGURATION ---
        
        # 1. Suppression des caractères invisibles (BOM)
        config_stations.columns = config_stations.columns.astype(str).str.replace('\ufeff', '').str.strip()
        
        # 2. Correction si le fichier CSV n'a pas été séparé correctement
        if len(config_stations.columns) == 1 and ';' in config_stations.columns[0]:
            print("Mauvais separateur detecte pour la configuration. Correction automatique...", flush=True)
            config_stations = config_stations[config_stations.columns[0]].str.split(';', expand=True)
            
        # 3. Forçage des colonnes avec un nom standard et en majuscules
        config_stations.columns = ['LIGNE', 'STATION_NUM', 'OPERATION']

        # 4. Conversion de la colonne Station_Num en nombre pour pouvoir la trier correctement
        config_stations['STATION_NUM'] = pd.to_numeric(config_stations['STATION_NUM'], errors='coerce')

    except Exception as e:
        print(f"Erreur : Impossible de preparer la table de configuration. {e}", flush=True)
        return

    generer_planning_ligne(engine, "NLG", "build_nlg_raw", "planning_nlg_grafana", cal, config_stations, semaine, annee)
    generer_planning_ligne(engine, "MLG", "build_mlg_raw", "planning_mlg_grafana", cal, config_stations, semaine, annee)

    print("Fin de l'execution des calculs.", flush=True)
    
# --- INFRASTRUCTURE (Inchangée) ---

def wait_for_db():
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    for i in range(30):
        try:
            with engine.connect() as conn: conn.execute(text("SELECT 1")); return engine
        except: time.sleep(2)
    raise Exception("DB Down")

# --- NOUVEAU DETECTEUR DE SEPARATEUR ---
def detecter_separateur(filepath):
    # Ouvre le fichier juste pour lire la première ligne
    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            if line.strip(): # On prend la première ligne non vide
                # S'il y a plus de points-virgules que de virgules, c'est un fichier français
                return ';' if line.count(';') > line.count(',') else ','
    return ',' # Par défaut

# --- NOUVEAU DETECTEUR DE SEPARATEUR ---
def detecter_separateur(filepath):
    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            if line.strip(): 
                return ';' if line.count(';') > line.count(',') else ','
    return ',' 

def import_csvs(engine):
    for csv_file, table in FILES_MAPPING.items():
        path = os.path.join(DATA_FOLDER, csv_file)
        if os.path.exists(path):
            print(f"⏳ Lecture de {csv_file} en cours...", flush=True)
            try:
                vrai_sep = detecter_separateur(path)
                
                # --- GESTION SPECIFIQUE DES FICHIERS ---
                if "Build_MPS_Pulse" in csv_file:
                    df = pd.read_csv(path, encoding='latin-1', sep=vrai_sep, low_memory=False, header=5)
                elif "DIActivity" in csv_file:
                    # ⚡️ ACCELERATEUR : On ne lit que les 50 000 premières lignes
                    df = pd.read_csv(path, encoding='latin-1', sep=vrai_sep, low_memory=False, nrows=50000)
                else:
                    df = pd.read_csv(path, encoding='latin-1', sep=vrai_sep, low_memory=False)
                
                # --- CORRECTION DU BUG "LIGNE" ---
                # On efface les caractères invisibles d'Excel et les espaces en trop
                df.columns = df.columns.astype(str).str.strip().str.replace('\ufeff', '')
                
                nom_table = table.lower()
                
                # Nettoyage de la BDD
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
                    conn.execute(text(f"DROP TABLE IF EXISTS `{nom_table}`"))
                    conn.commit()
                    
                # Envoi
                df.to_sql(nom_table, engine, if_exists='append', index=False)
                print(f"✅ Table '{nom_table}' créée avec succès !", flush=True)
                
            except Exception as e:
                print(f"❌ ERREUR sur {csv_file} : {e}", flush=True)

if __name__ == "__main__":
    print("--- Démarrage de l'ETL ---")
    eng = wait_for_db()
    
    # 1. On tente l'importation
    import_csvs(eng)
    
    # 2. PETITE VERIFICATION DE SECURITE
    # On regarde si la table build_nlg_raw existe bien avant de lancer la suite
    inspector = pd.read_sql("SHOW TABLES LIKE 'build_nlg_raw'", eng)
    
    if not inspector.empty:
        print("✅ Tables trouvées, lancement des calculs...")
        executer_mes_calculs(eng)
    else:
        print("❌ ERREUR : La table 'build_nlg_raw' est absente.")
        print("Vérifie que le fichier 'Build_MPS_Pulse_NLG.csv' est bien présent dans le dossier /data")


