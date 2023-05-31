import unittest
import pandas as pd
import os

folder_path = 'extracts/'
def check_empty_fields(filepath, columns):
    # Read csv from file
    df = pd.read_csv(filepath)

    # Check if there are any empty or NA fields in specific columns
    na_rows = df[columns].isna().any(axis=1)

    if na_rows.sum() > 0:
        # Write a csv with the rows that contain empty or NA fields
        null_rows_filepath = f"tests/{os.path.splitext(os.path.basename(filepath))[0]}_nullrows.csv"
        df[na_rows].to_csv(null_rows_filepath, index=False)

        return False

    return True


class TestCSVComparison(unittest.TestCase):
    def test_csv_comparison(self):
        # Charger les fichiers CSV en DataFrame pandas
        df2 = pd.read_csv(f"{folder_path}consultations.csv")
        df1 = pd.read_csv(f"{folder_path}prescriptions.csv")

        # Fusionner les deux DataFrames sur les clés primaires
        merged_df = pd.merge(df1, df2, left_on="consultation_import_identifier", right_on="import_identifier")

        # Vérifier que les valeurs de la colonne "patient_import_identifier" sont égales
        self.assertTrue((merged_df["patient_import_identifier_x"] == merged_df["patient_import_identifier_y"]).all())


class TestCSVComparison_2(unittest.TestCase):
    def test_csv_comparison(self):
        # Charger les fichiers CSV en DataFrame pandas
        df1 = pd.read_csv(f"{folder_path}treatments.csv")
        df2 = pd.read_csv(f"{folder_path}prescriptions.csv")

        # Fusionner les deux DataFrames sur les clés primaires
        merged_df = pd.merge(df1, df2, left_on="prescription_import_identifier", right_on="import_identifier")

        # Vérifier que les valeurs de la colonne "patient_import_identifier" sont égales
        self.assertTrue((merged_df["patient_import_identifier_x"] == merged_df["patient_import_identifier_y"]).all())

class TestCSVComparison_3(unittest.TestCase):
    def test_csv_comparison(self):
        # Charger les fichiers CSV en DataFrame pandas
        df1 = pd.read_csv(f"{folder_path}vaccine_injections.csv")
        df2 = pd.read_csv(f"{folder_path}consultations.csv")

        # Fusionner les deux DataFrames sur les clés primaires
        merged_df = pd.merge(df1, df2, left_on="consultation_import_identifier", right_on="import_identifier")
        
        # Vérifier que les valeurs de la colonne "patient_import_identifier" sont égales
        self.assertTrue((merged_df["patient_import_identifier_x"] == merged_df["patient_import_identifier_y"]).all())

class TestCSVComparison_4(unittest.TestCase):
    def test_csv_comparison(self):
        # Charger les fichiers CSV en DataFrame pandas
        df1 = pd.read_csv(f"{folder_path}patient_observations.csv")
        df2 = pd.read_csv(f"{folder_path}consultations.csv")

        # Fusionner les deux DataFrames sur les clés primaires
        merged_df = pd.merge(df1, df2, left_on="consultation_import_identifier", right_on="import_identifier")

        # Vérifier que les valeurs de la colonne "patient_import_identifier" sont égales
        self.assertTrue((merged_df["patient_import_identifier_x"] == merged_df["patient_import_identifier_y"]).all())



class CheckEmptyFields_treatments(unittest.TestCase):
    def test_check_empty_na_fields(self):
        filepath = f"{folder_path}treatments.csv"
        columns_to_check = ['import_identifier', 'start_date', 'end_date', 'prescription_import_identifier', 'patient_import_identifier']
        self.assertTrue(check_empty_fields(filepath, columns_to_check))

class CheckEmptyFields_prescriptions(unittest.TestCase):
    def test_check_empty_na_fields(self):
        filepath = f"{folder_path}prescriptions.csv"
        columns_to_check = ['import_identifier', 'created_at', 'updated_at', 'patient_import_identifier']
        self.assertTrue(check_empty_fields(filepath, columns_to_check))

class CheckEmptyFields_consultations(unittest.TestCase):
    def test_check_empty_na_fields(self):
        filepath = f"{folder_path}consultations.csv"
        columns_to_check = [
                            "import_identifier",
                            "patient_import_identifier",
                            "started_at"
                        ]
        self.assertTrue(check_empty_fields(filepath, columns_to_check))

class CheckEmptyFields_patients(unittest.TestCase):
    def test_check_empty_na_fields(self):
        filepath = f"{folder_path}patients.csv"
        columns_to_check = [    "import_identifier",
                                "birthdate"
                            ]
        self.assertTrue(check_empty_fields(filepath, columns_to_check))

class CheckEmptyFields_vaccine_injections(unittest.TestCase):
    def test_check_empty_na_fields(self):
        filepath = f"{folder_path}vaccine_injections.csv"
        columns_to_check = [    "import_identifier",
                                "imported_vaccine_name",
                                "injection_date"
                            ]
        self.assertTrue(check_empty_fields(filepath, columns_to_check))


if __name__ == '__main__':
    unittest.main()
