import pandas as pd
import swifter

observation_to_consultations = {
    "Motif ": "reason",
    "Examen": "illness_observation",
    "Commentaires": None,
    "Plainte nouvelle": "reason",
    "Etat dossier": None,
    "Diagnostic probable": "conclusion",
    "Commentaire": "conclusion",
}


def process_row(row):
    """
    Permet de à partir des champs display_name et value d'observations de remplir les champs
    reason,medical_assessment,illness_observation,conclusion de consultations
    """
    display_names = row["display_name"]
    values = row["value"]
    if isinstance(display_names, list):
        for index, name in enumerate(display_names):
            if observation_to_consultations[name]:
                column_name = observation_to_consultations[name]
                existing_value = row[column_name]
                if pd.notnull(existing_value) and str(values[index]) != existing_value:
                    row[column_name] = existing_value + "\n" + str(values[index])
                else:
                    row[column_name] = values[index]
    return row


def run(renamed_observations_df, renamed_consultation_df):
    # Créer une liste des clés du dictionnaire observation_to_consultations
    keys_to_consult = list(observation_to_consultations.keys())

    # Filtrer la dataframe en utilisant la méthode isin()
    filtered_observations_df = renamed_observations_df[
        renamed_observations_df["display_name"].isin(keys_to_consult)
    ]
    filtered_columns_observations_df = filtered_observations_df[
        ["consultation_import_identifier", "display_name", "value"]
    ]
    grouped_observations_df = (
        filtered_columns_observations_df.groupby("consultation_import_identifier")
        .agg({"display_name": list, "value": list})
        .reset_index()
    )
    merged_df = pd.merge(
        renamed_consultation_df,
        grouped_observations_df,
        left_on="import_identifier",
        right_on="consultation_import_identifier",
        how="left",
    )
    merged_df = merged_df.swifter.apply(process_row, axis=1)
    # on génére les 2 dataframes observations et consultations finales
    observation_clean_df = renamed_observations_df[
        ~renamed_observations_df["display_name"].isin(keys_to_consult)
    ]
    # on supprime les 2 colonnes qui venaient d'observation
    merged_df.drop(columns=["value", "display_name"], inplace=True)
    return observation_clean_df, merged_df


if __name__ == "__main__":
    renamed_observations_df = pd.read_csv("extracts/patient_observationscsv")
    renamed_consultation_df = pd.read_csv("extracts/consultations.csv")
    observation_clean_df, consultations_ettofed_df = run(
        renamed_observations_df, renamed_consultation_df
    )
    observation_clean_df.to_csv("observation_clean.csv", index=False)
    consultations_ettofed_df.to_csv("consultation_etoffed.csv", index=False)
