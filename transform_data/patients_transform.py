import pandas as pd

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


def parse_textcons_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            line_data = {}
            if line.startswith('NumCons;'):
                parts = line.split('UUID;')
                numcons_part = parts[0]
                uuid_part = parts[1] if len(parts) > 1 else ''
                
                numcons_and_text = numcons_part.split('\t', 1)
                if len(numcons_and_text) == 2:
                    numcons, text = numcons_and_text
                    line_data[numcons.split(';')[0]] = numcons.split(';')[1]
                    line_data['TextCons_'] = text
                else:
                    continue
                
                line_data['UUID'] = uuid_part.strip()

            if line_data:
                data.append(line_data)

    return data





patients_data = parse_data('data/PAT_Patients.txt')
secu_data = parse_data('data/PAT_Secu.txt')
addresses_data = parse_data('data/PAT_Adresses.txt')

# Convert list of dictionaries to pandas DataFrame
patients_df = pd.DataFrame(patients_data)
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
text_cons_data = parse_textcons_data('data/PAT_TextCons.txt')

# Convert list of dictionaries to pandas DataFrame
consult_df = pd.DataFrame(consult_data)
text_cons_df = pd.DataFrame(text_cons_data)
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
