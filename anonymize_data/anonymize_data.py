'''
Filename: /Users/matthieuarchambault/Documents/DCA/bromoula-hypermed/data_anonymisation.py
Path: /Users/matthieuarchambault/Documents/DCA/bromoula-hypermed
Created Date: Friday, April 14th 2023, 11:21:01 am
Author: Matthieu Archambault

Copyright (c) 2023 DCA
'''

import os
import pandas as pd
import names

# Déclaration des noms de colonnes
ssn_column = 'Ssn'
first_name_column = 'patient_first_name'
last_name_column = 'patient_last_name'
patient_id_column = 'patient_id'

def anonymize_patients_data(patients_data):
    patients_data[ssn_column] = 'XXX-XX-XXXX'
    patients_data[first_name_column] = patients_data[first_name_column].apply(lambda _: names.get_first_name())
    patients_data[last_name_column] = patients_data[last_name_column].apply(lambda _: names.get_last_name())
    return patients_data

def anonymize_other_table(other_table, patient_id_column, anonymized_patients_data):
    merged_data = other_table.merge(anonymized_patients_data[[patient_id_column, ssn_column, first_name_column, last_name_column]], on=patient_id_column, how='left')
    for column in [ssn_column, first_name_column, last_name_column]:
        other_table.loc[merged_data[column].notnull(), column] = merged_data[column]
    return other_table

input_folder = 'input_csv_folder'
output_folder = 'output_csv_folder'

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Process the patients.csv file
patients_data = pd.read_csv(os.path.join(input_folder, 'patients.csv'))
anonymized_patients_data = anonymize_patients_data(patients_data)
anonymized_patients_data.to_csv(os.path.join(output_folder, 'anonymized_patients.csv'), index=False)

# Process the first 100 patients
first_100_patients = anonymized_patients_data.head(100)
first_100_patients.to_csv(os.path.join(output_folder, 'first_100_anonymized_patients.csv'), index=False)

# Process other tables
for file in os.listdir(input_folder):
    if file.endswith('.csv') and file != 'patients.csv':
        other_table_data = pd.read_csv(os.path.join(input_folder, file))
        anonymized_other_table_data = anonymize_other_table(other_table_data, patient_id_column, first_100_patients)
        anonymized_other_table_data.to_csv(os.path.join(output_folder, f'anonymized_{file}'), index=False)
