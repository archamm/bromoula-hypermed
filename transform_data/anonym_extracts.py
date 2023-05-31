import pandas as pd
import os
import names

# CSV file paths
patients_csv = 'extracts/patients.csv'
extracts_folder = 'extracts'
output_folder = 'extract_anonym'

# Read the patients CSV file
df_patients = pd.read_csv(patients_csv, dtype={'patient_import_identifier': int})

# Generate fake names for patients.csv
df_patients['fake_last_name'] = [names.get_last_name() for _ in range(len(df_patients))]
df_patients['fake_first_name'] = [names.get_first_name() for _ in range(len(df_patients))]

# Anonymize the patients.csv file
df_patients['last_name'] = df_patients['fake_last_name']
df_patients['first_name'] = df_patients['fake_first_name']
df_patients['maiden_name'] = 'Nom de jeune fille'
df_patients['birthdate'] = 'Date de naissance'
df_patients['occupation'] = 'Profession'
df_patients['phone1'] = '0600000000'
df_patients['email1'] = 'email1@example.com'
df_patients['email2'] = 'email2@example.com'
df_patients['regular_doctor_name'] = 'Nom du médecin traitant'
df_patients['address'] = 'Adresse'
df_patients['zipcode'] = 'Code postal'
df_patients['city'] = 'Ville'
df_patients['notes'] = 'Notes'
df_patients['ssn'] = 'Numéro de sécurité sociale'
df_patients['phone2'] = '0600000000'
df_patients['phone3'] = '0600000000'
df_patients['phone4'] = '0600000000'

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Anonymize the other CSV files in the "extracts" folder
for filename in os.listdir(extracts_folder):
    
    if filename != 'patients.csv' and filename.endswith('.csv'):
        print(filename)
        filepath = os.path.join(extracts_folder, filename)
        df = pd.read_csv(filepath, dtype={'patient_import_identifier': int})
        print(len(df))
        # Merge with the patients.csv file based on "patient_import_identifier"
        df = df.merge(df_patients[['import_identifier', 'fake_last_name', 'fake_first_name']],
                      left_on='patient_import_identifier', right_on='import_identifier', how='left')
        print(len(df))
        # Replace "patient_last_name" and "patient_first_name" with the fake names
        df['patient_last_name'] = df['fake_last_name']
        df['patient_first_name'] = df['fake_first_name']
        # Rename the 'import_identifier_x' column to 'import_identifier'
        df = df.rename(columns={'import_identifier_x': 'import_identifier'})
        df.drop(['fake_last_name', 'fake_first_name'], axis=1, inplace=True)
        if 'import_identifier_y' in df.columns:
            df = df.drop('import_identifier_y', axis=1)
        output_filepath = os.path.join(output_folder, filename)
        df.to_csv(output_filepath, index=False)

# Write the anonymized patients.csv file to the output folder
output_patients_csv = os.path.join(output_folder, 'patients.csv')
df_patients.to_csv(output_patients_csv, index=False)
