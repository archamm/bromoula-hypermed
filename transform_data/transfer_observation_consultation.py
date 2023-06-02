import pandas as pd
import multiprocessing
from tqdm import tqdm
import numpy as np
import itertools

observation_to_consultations = {
    "Motif ": "reason",
    "Examen": "illness_observation",
    "Commentaires": None,
    "Plainte nouvelle" : "reason",
    "Etat dossier" : None,
    "Diagnostic probable" : "conclusion",
    "Commentaire": "conclusion"
}

def process_row(row, observation_to_consultations, renamed_consultation_df):
    """
    Traitement pour une seule ligne de données.
    
    Args:
        row (pandas.Series): Ligne de données à traiter.
        observation_to_consultations (dict): Dictionnaire contenant les correspondances entre les valeurs de 'display_name' et les noms de colonnes dans 'renamed_consultation_df'.
        renamed_consultation_df (pandas.DataFrame): DataFrame contenant les données de consultation.
    
    Returns:
        list: Liste des indices de lignes à supprimer.
    """
    rows_to_delete = []
    display_name = row['display_name']
    value = row['value']
    consultation_import_identifier = row['consultation_import_identifier']
    if display_name in observation_to_consultations and value and str(value).strip() and pd.notnull(value) and observation_to_consultations[display_name]:
        column_name = observation_to_consultations[display_name]
        existing_value = renamed_consultation_df.loc[renamed_consultation_df['import_identifier'] == consultation_import_identifier, column_name].values[0]
        if pd.notnull(existing_value):
            value = existing_value + '\n' + value
        renamed_consultation_df.loc[renamed_consultation_df['import_identifier'] == consultation_import_identifier, column_name] = value
        rows_to_delete.append(row.name)
    return rows_to_delete

def process_chunk(chunk, observation_to_consultations, renamed_consultation_df):
    """
    Traitement pour un chunk de données.
    
    Args:
        chunk (pandas.DataFrame): Chunk de données à traiter.
        observation_to_consultations (dict): Dictionnaire contenant les correspondances entre les valeurs de 'display_name' et les noms de colonnes dans 'renamed_consultation_df'.
        renamed_consultation_df (pandas.DataFrame): DataFrame contenant les données de consultation.
    
    Returns:
        list: Liste des indices de lignes à supprimer.
    """

    rows_to_delete = chunk.apply(process_row, axis=1, args=(observation_to_consultations, renamed_consultation_df))
    rows_to_delete = list(itertools.chain(*rows_to_delete))  # Aplatir la liste
    return rows_to_delete


def add_values_to_dataframe(observation_to_consultations, renamed_observations_df, renamed_consultation_df):
    """
    Cette méthode lit les lignes de la DataFrame 'renamed_observations_df' et extrait les valeurs correspondantes
    en fonction des clés du dictionnaire. Ces valeurs sont ensuite ajoutées avec un retour à la ligne dans les
    champs correspondants de la DataFrame 'renamed_consultation_df'. De plus, si une clé du dictionnaire est présente
    dans le champ 'display_name' de 'renamed_observations_df', la ligne correspondante est supprimée de la DataFrame.
    
    Args:
        renamed_observations_df (pandas.DataFrame): DataFrame contenant les valeurs à extraire.
        renamed_consultation_df (pandas.DataFrame): DataFrame où les valeurs extraites seront ajoutées.
    
    Returns:
        pandas.DataFrame: renamed_consultation_df et renamed_observations_df
    """
    chunks = np.array_split(renamed_observations_df, multiprocessing.cpu_count())  # Diviser les données en chunks
    pool = multiprocessing.Pool()
    results = []
    with tqdm(total=len(chunks)) as pbar:
        for chunk in chunks:
            result = pool.apply_async(process_chunk, args=(chunk, observation_to_consultations, renamed_consultation_df))
            result.get()
            results.append(result)
            pbar.update(1)
    pool.close()
    pool.join()
    rows_to_delete = [result.get() for result in results]
    rows_to_delete = list(itertools.chain(*rows_to_delete))  # Aplatir la liste
    renamed_observations_df = renamed_observations_df.drop(rows_to_delete)
    return renamed_consultation_df, renamed_observations_df


def main():
    renamed_observations_df = pd.read_csv('extracts/patient_observations test.csv')
    renamed_consultation_df = pd.read_csv('extracts/consultations test.csv')

    renamed_consultation_df, renamed_observations_df = add_values_to_dataframe(observation_to_consultations, renamed_observations_df, renamed_consultation_df)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
