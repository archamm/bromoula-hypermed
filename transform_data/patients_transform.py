import pandas as pd
import re

regex_pattern_text_cons = r'TextCons_;(.*?)\tUUID;'
regex_pattern_num_cons = r'NumCons;(.*?)\tTextCons_;'



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






patients_data = parse_data('data/PAT_Patients.txt')
secu_data = parse_data('data/PAT_Secu.txt')
addresses_data = parse_data('data/PAT_Adresses.txt')

# Convert list of dictionaries to pandas DataFrame
patients_df = pd.DataFrame(patients_data)
print(patients_df.columns)
secu_df = pd.DataFrame(secu_data)
addresses_df = pd.DataFrame(addresses_data)

# Merge the dataframes
merged_df = pd.merge(patients_df, secu_df, left_on='ID', right_on='CodePat')
merged_df = pd.merge(merged_df, addresses_df, on='CodePat')
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

# Save the filtered and renamed dataframe to a .csv file
renamed_df.to_csv('extracts/patients.csv', index=False, header=True)


# Parse the data
consult_data = parse_data('data/PAT_Consult.txt')

# Convert list of dictionaries to pandas DataFrame
consult_df = pd.DataFrame(consult_data)
text_cons_df = get_df_from_textcons()
print(text_cons_df.head())
print(consult_df.head())

# Merge the dataframes on NumCons
merged_df = pd.merge(consult_df, text_cons_df, on='NumCons')
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
print(merged_df.head())

# Select only required columns from dataframe
filtered_df = merged_df[column_mapping.keys()]

# Rename the columns to match the destination schema
renamed_df = filtered_df.rename(columns=column_mapping)

# Add missing columns
missing_columns = ['illness_observation', 'conclusion']
for col in missing_columns:
    renamed_df[col] = ""

# Use 'Titre' as 'conclusion' as well
renamed_df['conclusion'] = renamed_df['reason']
renamed_df['finished_at'] = renamed_df['started_at']

# Save the filtered and renamed dataframe to a .csv file
renamed_df.to_csv('extracts/consultations.csv', index=False, header=True, escapechar="\\")

pat_vac_data = parse_data('data/PAT_Vaccins.txt')
per_vac_data = parse_data('data/PER_Vaccins.txt')

# Convert list of dictionaries to pandas DataFrame
pat_vac_df = pd.DataFrame(pat_vac_data)
per_vac_df = pd.DataFrame(per_vac_data)
print(pat_vac_df.head())
print(per_vac_df.head())

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
    'TypeVac_y': 'referential_type',
    'Nlot': 'batch_number',
    'Vaccin': 'imported_vaccine_name',
}

print(merged_df.head())



# Select only required columns from dataframe
filtered_df = merged_df[column_mapping_pat.keys()]


# Rename the columns to match the destination schema
renamed_df = filtered_df.rename(columns=column_mapping_pat)
print(renamed_df.head())

renamed_df['imported_disease_name'] = ''   # Replace with actual values if available
renamed_df['injection_location'] = ''  # Replace with actual values if available
renamed_df['consultation_import_identifier'] = ''   # Replace with actual values if available
renamed_df['referential_id'] = ''   # Replace with actual values if available
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


renamed_df.to_csv('extracts/vaccines.csv', index=False, header=True)



def get_patient_name(num_pat, patients_df):
    # Find the patient in the DataFrame
    patient = patients_df[patients_df['ID'] == num_pat]
    if len(patient) > 0:
        return patient['first_name'].values[0], patient['last_name'].values[0]
    else:
        return '', ''

# Parse the data
medical_history_data = parse_data('data/PAT_ATCDPerso.txt')

# Convert list of dictionaries to pandas DataFrame
medical_history_df = pd.DataFrame(medical_history_data)

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
print(medical_history_df.head)
# Select only required columns from dataframe
filtered_df = medical_history_df[column_mapping_atcd.keys()]



# Rename the columns to match the destination schema
renamed_df = filtered_df.rename(columns=column_mapping_atcd)
renamed_df['enrichment_data'] = ''  # Replace with actual values if available
renamed_df['category'] = 'Medical'  
renamed_df['import_identifier'] =  renamed_df.index 

print(renamed_df.head())

renamed_df = renamed_df.dropna(subset=['patient_import_identifier'])
renamed_df = renamed_df.dropna(subset=['title'])

renamed_df.to_csv('extracts/medical_history_lines.csv', index=False, header=True)


 
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

# Saving to CSV
observations_df.to_csv('extracts/patient_observations.csv', index=False, header=True)