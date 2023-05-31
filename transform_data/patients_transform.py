import pandas as pd
import re
import logging

# Configure logging to write logs to a file
logging.basicConfig(filename='hypermed_to_docto_generation.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

regex_pattern_text_cons = r'TextCons_;(.*?)\tUUID;'
regex_pattern_num_cons = r'NumCons;(.*?)\tTextCons_;'


# Custom function to update the 'end_date' column
def update_end_date(row):
    if pd.isnull(row['end_date']) or row['end_date'] == '':
        start_date = pd.to_datetime(row['start_date'], format='%d/%m/%Y')
        new_end_date = (start_date + pd.DateOffset(years=1)).strftime('%d/%m/%Y')
        return new_end_date
    else:
        return row['end_date']

def get_text_cons_list(input_data):
    matches = re.findall(regex_pattern_text_cons, input_data, re.DOTALL)
    return matches

def get_num_cons_list(input_data):
    matches = re.findall(regex_pattern_num_cons, input_data, re.DOTALL)
    return matches

def generate_dataframe_for_textCons(input_data):
    numcons_list = get_num_cons_list(input_data)
    textcons_list = get_text_cons_list(input_data)    
    df = pd.DataFrame({'NumCons': numcons_list, 'TextCons_': textcons_list})
    return df

def remove_comma_from_files(file_path):
    with open(file_path, 'r') as file:
        text = file.read()
        text = text.replace(',', '')  # Remove commas from the text

    return text

def get_df_from_textcons():
    file_path = "data/PAT_TextCons.txt"
    data = remove_comma_from_files(file_path)
    return generate_dataframe_for_textCons(data)

def parse_data(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    data = []
    for line in lines:
        line_data = {}
        entries = line.split('\t')
        for entry in entries:
            key_value = entry.split(';')
            if len(key_value) == 2:
                key, value = key_value
                line_data[key] = value
        data.append(line_data)
    return data




csv_logger = logging.getLogger('CSVGeneration')


patients_data = parse_data('data/PAT_Patients.txt')
csv_logger.info(f"Loaded data from PAT_Adresses.txt")

secu_data = parse_data('data/PAT_Secu.txt')
csv_logger.info(f"Loaded data from PAT_Secu.txt")

addresses_data = parse_data('data/PAT_Adresses.txt')
csv_logger.info(f"Loaded data from PAT_Adresses.txt")

# Convert list of dictionaries to pandas DataFrame
patients_df = pd.DataFrame(patients_data)
secu_df = pd.DataFrame(secu_data)
addresses_df = pd.DataFrame(addresses_data)

# Merge the dataframes
merged_df = pd.merge(patients_df, secu_df, left_on='ID', right_on='CodePat', how='left')
merged_df = pd.merge(merged_df, addresses_df, on='CodePat', how='left')
merged_df = merged_df.drop_duplicates(keep='first', subset='ID')

# Define mapping between original column names and new names
column_mapping = {
    'ID': 'import_identifier',
    'Sexe': 'gender',
    'Nom': 'last_name',
    'Prenom': 'first_name',
    'NomFille': 'maiden_name',
    'DateNais': 'birthdate',
    'Profession': 'occupation',
    'Tel': 'phone1',   
    'email': 'email1', 
    'email2': 'email2', 
    'NomMT': 'regular_doctor_name',
    'Adresse': 'address',
    'CodePostal': 'zipcode',
    'Commune': 'city',
    'Notes': 'notes',
    'NoSS': 'ssn'
}

# Select only required columns from dataframe
filtered_df = merged_df[column_mapping.keys()]

# Rename the columns to match the destination schema
renamed_df = filtered_df.rename(columns=column_mapping)

# Add missing columns
missing_columns = ['phone2', 'phone3', 'phone4']
for col in missing_columns:
    renamed_df[col] = ""
renamed_df = renamed_df.dropna(subset=['import_identifier'])

# Save the filtered and renamed dataframe to a .csv file
renamed_df.to_csv('extracts/patients.csv', index=False, header=True)
csv_logger.info(f"Generated patients.csv")

# Parse the data
consult_data = parse_data('data/PAT_Consult.txt')

# Convert list of dictionaries to pandas DataFrame
consult_df = pd.DataFrame(consult_data)
print(len(consult_df))
text_cons_df = get_df_from_textcons()
csv_logger.info(f"Loaded data from PAT_Consult.txt and PAT_TextCons.txt")

# Merge the dataframes on NumCons
merged_df = pd.merge(consult_df, text_cons_df, on='NumCons', how='left')

# Add patient_first_name and patient_last_name from patients_df
merged_df = pd.merge(merged_df, patients_df[['ID', 'Prenom', 'Nom']], left_on='NumPat', right_on='ID')
# Define mapping between original column names and new names
column_mapping = {
    'NumCons': 'import_identifier',
    'ID': 'patient_import_identifier',
    'Nom': 'patient_last_name',
    'Prenom': 'patient_first_name',
    'Datecons': 'started_at',
    'Titre': 'reason',
    'TextCons__y': 'medical_assessment',
}

# Select only required columns from dataframe
filtered_df = merged_df[column_mapping.keys()]

# Rename the columns to match the destination schema
renamed_consultation_df = filtered_df.rename(columns=column_mapping)

# Add missing columns
missing_columns = ['illness_observation', 'conclusion']
for col in missing_columns:
    renamed_consultation_df[col] = ""

# Use 'Titre' as 'conclusion' as well
renamed_consultation_df['conclusion'] = renamed_consultation_df['reason']
renamed_consultation_df['finished_at'] = renamed_consultation_df['started_at']

# Save the filtered and renamed dataframe to a .csv file
renamed_consultation_df = renamed_consultation_df.drop_duplicates()
renamed_consultation_df = renamed_consultation_df.dropna(subset=['import_identifier'])

print(len(renamed_consultation_df))
renamed_consultation_df.to_csv('extracts/consultations.csv', index=False, header=True, escapechar="\\")
csv_logger.info(f"Generated consultations.csv")

pat_vac_data = parse_data('data/PAT_Vaccins.txt')
per_vac_data = parse_data('data/PER_Vaccins.txt')

# Convert list of dictionaries to pandas DataFrame
pat_vac_df = pd.DataFrame(pat_vac_data)
per_vac_df = pd.DataFrame(per_vac_data)
csv_logger.info(f"Loaded data from PAT_Vaccins.txt and PER_Vaccins.txt")

# Merge the dataframes on NumCons
merged_df = pd.merge(pat_vac_df, per_vac_df, on='CodeVac')
# Add patient_first_name and patient_last_name from patients_df
merged_df = pd.merge(merged_df, patients_df[['ID', 'Prenom', 'Nom']], left_on='Numpat', right_on='ID')

# Define mapping between original column names and new names
column_mapping_pat = {
    'ID': 'patient_import_identifier',
    'Nom': 'patient_last_name',
    'Prenom': 'patient_first_name',
    'DateVac': 'injection_date',
    'Nlot': 'batch_number',
    'Vaccin': 'imported_vaccine_name',
}




# Select only required columns from dataframe
filtered_df = merged_df[column_mapping_pat.keys()]


# Rename the columns to match the destination schema
renamed_df = filtered_df.rename(columns=column_mapping_pat)

renamed_df['imported_disease_name'] = ''   # Replace with actual values if available
renamed_df['injection_location'] = ''  # Replace with actual values if available
renamed_df['consultation_import_identifier'] = ''   # Replace with actual values if available
renamed_df['referential_id'] = ''   # Replace with actual values if available
renamed_df['referential_type'] = ''   # Replace with actual values if available
renamed_df['import_identifier'] = merged_df.index  # Replace with actual values if available
renamed_df['other_doctor_name'] = ''  # Replace with actual values if available

schema = [
    'import_identifier',
    'consultation_import_identifier',
    'patient_import_identifier',
    'patient_first_name',
    'patient_last_name',
    'imported_vaccine_name',
    'imported_disease_name',
    'referential_id',
    'referential_type',
    'injection_date',
    'batch_number',
    'injection_location',
    'other_doctor_name'
]

# Filter the DataFrame to include only columns present in the schema
renamed_df = renamed_df.filter(items=schema)


renamed_df.to_csv('extracts/vaccine_injections.csv', index=False, header=True)
csv_logger.info(f"Generated vaccines.csv")


# Parse the data
medical_history_data = parse_data('data/PAT_ATCDPerso.txt')

# Convert list of dictionaries to pandas DataFrame
medical_history_df = pd.DataFrame(medical_history_data)
csv_logger.info(f"Loaded data from PAT_ATCDPerso.txt")

# Add patient_first_name and patient_last_name from patients_df
medical_history_df = pd.merge(medical_history_df, patients_df[['ID', 'Prenom', 'Nom']], left_on='NumPat', right_on='ID')

# Define mapping between original column names and new names
column_mapping_atcd = {
    'ID': 'patient_import_identifier',
    'Nom': 'patient_last_name',
    'Prenom': 'patient_first_name',
    'JourATCD': 'start_day',
    'MoisATCD': 'start_month',
    'AnATCD': 'start_year',
    'JourATCD': 'end_day',
    'MoisATCD': 'end_month',
    'AnATCD': 'end_year',
    'Titre': 'title',
    'TexATCD': 'content',
    'CodCIM': 'code',
}
# Select only required columns from dataframe
filtered_df = medical_history_df[column_mapping_atcd.keys()]



# Rename the columns to match the destination schema
renamed_df = filtered_df.rename(columns=column_mapping_atcd)
renamed_df['enrichment_data'] = ''  # Replace with actual values if available
renamed_df['category'] = 'Medical'  
renamed_df['import_identifier'] =  renamed_df.index 


renamed_df = renamed_df.dropna(subset=['patient_import_identifier'])
renamed_df = renamed_df.dropna(subset=['title'])

renamed_df.to_csv('extracts/medical_history_lines.csv', index=False, header=True)
csv_logger.info(f"Generated medical_history_lines.csv")

 
# Filtering out the rows where both 'Poids' and 'Taille' are not equal to 0
filtered_patient_df = patients_df[(patients_df['Poids'] != 0) | (patients_df['Taille'] != 0)]

# Creating two separate DataFrames for 'Poids' and 'Taille'
df_poids = filtered_patient_df[filtered_patient_df['Poids'] != "0"][['ID', 'Nom', 'Prenom', 'DateCreat', 'Poids']]
df_taille = filtered_patient_df[filtered_patient_df['Taille'] != "0"][['ID', 'Nom', 'Prenom', 'DateCreat', 'Taille']]

# Adding new columns to both DataFrames
df_poids['display_name'] = 'Poids du patient'
df_poids['measurement_unit'] = 'kg'
df_poids['code'] = ''
df_poids['value'] = df_poids['Poids']

df_taille['display_name'] = 'Taille du patient'
df_taille['measurement_unit'] = 'cm'  # Assuming the unit here
df_taille['code'] = ''  # Replace with actual code
df_taille['value'] = df_taille['Taille']

# Combining the two DataFrames
observations_df = pd.concat([df_poids, df_taille])
observations_df['consultation_import_identifier'] = ''
# Renaming the columns
observations_df.rename(columns={'ID': 'patient_import_identifier', 
                         'Nom': 'patient_last_name', 
                         'Prenom': 'patient_first_name', 
                         'DateCreat': 'recorded_at'}, inplace=True)

# Rearranging the columns to match your desired format
observations_df = observations_df[['consultation_import_identifier',
                     'patient_import_identifier', 
                     'patient_first_name', 
                     'patient_last_name', 
                     'value', 
                     'recorded_at', 
                     'measurement_unit', 
                     'code', 
                     'display_name']]
observations_df = observations_df.drop(observations_df[observations_df['value'] == "0"].index)
observations_df = observations_df.dropna(subset=['patient_import_identifier'])
observations_df = observations_df.dropna(subset=['value'])

# Saving to CSV
observations_df.to_csv('extracts/patient_observations.csv', index=False, header=True)
csv_logger.info(f"Generated patient_observations.csv")

# Parse the data
prescription_data = parse_data('data/PAT_OrdCons.txt')

# Convert list of dictionaries to pandas DataFrame
prescription_df = pd.DataFrame(prescription_data)
csv_logger.info(f"Loaded data from PAT_OrdCons.txt")

# Add patient_first_name and patient_last_name from patients_df
prescription_df = pd.merge(prescription_df, renamed_consultation_df[['import_identifier', 'patient_import_identifier', 'patient_first_name', 'patient_last_name']], left_on='NumCons', right_on='import_identifier')

# Define mapping between original column names and new names
column_mapping_prescription = {
    'NumCons': 'consultation_import_identifier',
    'patient_import_identifier': 'patient_import_identifier',
    'patient_last_name': 'patient_last_name',
    'patient_first_name': 'patient_first_name',
    'DateCons': 'created_at',
    'NomProduit': 'medication',
    'Poso': 'posology',
    'DateFinProd': 'end_date',
}

# Select only required columns from dataframe
filtered_df_prescription = prescription_df[column_mapping_prescription.keys()]

# Rename the columns to match the destination schema
renamed_df_prescription = filtered_df_prescription.rename(columns=column_mapping_prescription)



# Define a default start date based on the 'created_at' column
renamed_df_prescription['start_date'] = renamed_df_prescription['created_at']
renamed_df_prescription['updated_at'] = renamed_df_prescription['created_at']
renamed_df_prescription['import_identifier']= renamed_df_prescription.index

column_prescription_order = ['import_identifier', 'consultation_import_identifier', 'patient_import_identifier',
                'patient_first_name', 'patient_last_name', 'created_at', 'updated_at']

renamed_df_prescription = renamed_df_prescription.dropna(subset=['created_at'])

# Save the filtered and renamed dataframe to a .csv file
renamed_df_prescription[column_prescription_order].to_csv('extracts/prescriptions.csv', index=False, header=True)
csv_logger.info(f"Generated prescriptions.csv")

# Now let's create the DataFrame for the treatments

# Define mapping between original column names and new names for the treatments
column_mapping_treatment = {
    'patient_import_identifier': 'patient_import_identifier',
    'patient_last_name': 'patient_last_name',
    'patient_first_name': 'patient_first_name',
    'DateCons': 'start_date',
    'DateFinProd': 'end_date',
    'NomProduit': 'medication',
    'Poso': 'posology',
}

# Select only required columns from dataframe
filtered_df_treatment = prescription_df[column_mapping_treatment.keys()]

# Rename the columns to match the destination schema
renamed_df_treatment = filtered_df_treatment.rename(columns=column_mapping_treatment)

# Add missing columns
renamed_df_treatment['import_identifier']= renamed_df_treatment.index
renamed_df_treatment['prescription_import_identifier']= renamed_df_treatment.index
missing_columns_treatment = ['long_term_condition', 'long_term_treatment', 'raw_code', 'rank']
for col in missing_columns_treatment:
    renamed_df_treatment[col] = ""


renamed_df_treatment = renamed_df_treatment.dropna(subset=['start_date'])
renamed_df_treatment['end_date'] = renamed_df_treatment.apply(update_end_date, axis=1)

# Save the filtered and renamed dataframe to a .csv file
column_treatment_order = ['import_identifier', 'prescription_import_identifier', 'patient_import_identifier',
                'patient_first_name', 'patient_last_name', 'start_date', 'end_date', 'medication',
                'posology', 'long_term_condition', 'long_term_treatment', 'rank', 'raw_code']

renamed_df_treatment[column_treatment_order].to_csv('extracts/treatments.csv', index=False, header=True)
csv_logger.info(f"Generated treatments.csv")

documents_data = parse_data('data/PAT_PICTQUICK.txt')

# Convert list of dictionaries to pandas DataFrame
documents_df = pd.DataFrame(documents_data)
csv_logger.info(f"Loaded data from PAT_PICTQUICK.txt")

column_mapping_documents = {
    'NumCons': 'consultation_import_identifier',
    'Creat_Date': 'originally_created_on',
    'NomFichier': 'filename',
    'Titre': 'Titre',
}

# Select only required columns from dataframe
filtered_df_documents = documents_df[column_mapping_documents.keys()]

# Rename the columns to match the destination schema
renamed_df_documents = filtered_df_documents.rename(columns=column_mapping_documents)

missing_columns = ['practitioner_adeli_id', 'practitioner_rpps_id', 'practitioner_code', 'care_plan_import_identifier', 'kind', 'key', 'size', 'extension', 'conclusion']
for col in missing_columns:
    renamed_df_documents[col] = ""

renamed_df_documents['generated_during_import'] = False
renamed_df_documents['file_kit_tanker_uploaded'] = 0

courrier_df = pd.read_csv('courrier.csv', sep=',')
renamed_df_documents = pd.concat([courrier_df[
    ["practitioner_adeli_id",
    "practitioner_rpps_id",
    "practitioner_code",
    "consultation_import_identifier",
    "care_plan_import_identifier",
    "originally_created_on",
    "filename",
    "title",
    "kind",
    "other_key",
    "file_kit_tanker_uploaded",
    "size",
    "extension"]
], renamed_df_documents])

# Add patient_first_name and patient_last_name from renamed_consultation_df
renamed_df_documents = pd.merge(renamed_df_documents, renamed_consultation_df[['import_identifier', 'patient_import_identifier', 'patient_first_name', 'patient_last_name']], left_on='consultation_import_identifier', right_on='import_identifier')
renamed_df_documents['import_identifier'] = renamed_df_documents.index

column_documents_order = [
    'import_identifier',
    "patient_import_identifier",
    "patient_first_name",
    "patient_last_name",
    "practitioner_adeli_id",
    "practitioner_rpps_id",
    "practitioner_code",
    "consultation_import_identifier",
    "care_plan_import_identifier",
    "originally_created_on",
    "filename",
    "title",
    "kind",
    "other_key",
    "file_kit_tanker_uploaded",
    "size",
    "extension"
]
renamed_df_documents[column_documents_order].to_csv('extracts/documents.csv', index=False, header=True)
csv_logger.info(f"Generated documents.csv")

pd.DataFrame(columns=['patient_import_identifier', 'patient_first_name', 'patient_last_name', 'content']).to_csv('extracts/patient_memos.csv', index=False)
csv_logger.info(f"Generated patient_memos.csv")