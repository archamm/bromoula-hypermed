"""
Permet de créer un fichier courrier.csv au même format doctolib document.csv et de générer des pdf contenant les informations de chaque courrier.
"""

import re
import os
import pandas as pd
import zipfile
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

columns_document_df = ['import_identifier', 'patient_import_identifier', 'patient_first_name', 'patient_last_name', 'practitioner_adeli_id',
               'practitioner_rpps_id', 'practitioner_code', 'consultation_import_identifier', 'care_plan_import_identifier', 'originally_created_on',
                 'filename', 'title', 'kind', 'other_key', 'file_kit_tanker_uploaded', 'size', 'extension']


def extract_infos(balise1, balise2, data):
    pattern_regex = re.escape(balise1) + r'(.*?)\t' + re.escape(balise2)
    matches = re.findall(pattern_regex, data, re.DOTALL)
    return matches

def generate_courrier_csv(data):
    numcons_list = extract_infos("NumCons;", "TitreCour;", data)
    CodeCour_list = extract_infos("CodeCour;", "Sansentet;", data)
    TitreCour_list = extract_infos("TitreCour;", "lettre_;", data)
    filename_list = [f'courrier_{code}.pdf' for code in CodeCour_list]
    None_list = [None for code in CodeCour_list]
    

    df = pd.DataFrame({'import_identifier': CodeCour_list,
                   'patient_import_identifier': None_list,
                   'patient_first_name': None_list,
                   'patient_last_name': None_list,
                   'practitioner_adeli_id': None_list,
                   'practitioner_rpps_id': None_list,
                   'practitioner_code': None_list,
                   'consultation_import_identifier': numcons_list,
                   'care_plan_import_identifier': None_list,
                   'originally_created_on': None_list,
                   'filename': filename_list,
                   'title': TitreCour_list,
                   'kind': None_list,
                   'other_key': None_list,
                   'file_kit_tanker_uploaded': None_list,
                   'size': None_list,
                   'extension': None_list
                   })
    df.to_csv("courrier.csv", index=False)
    return filename_list

def generate_pdf(data, filename_list, output_folder):
    courrier_contenu_list = extract_infos("lettre_;", "CodeCour;", data)

    # Créer un dossier de sortie s'il n'existe pas déjà
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for i in range(len(filename_list)):
        filename = filename_list[i]
        courrier_contenu = courrier_contenu_list[i]

        # Créer un nouveau fichier PDF avec le nom de fichier de sortie donné
        output_path = os.path.join(output_folder, filename)
        c = canvas.Canvas(output_path, pagesize=letter)

        # Configuration de la police et de la taille de police pour le texte
        c.setFont("Helvetica", 12)

        # Définition de la position de départ (x, y) pour le texte
        x, y = 50, 750

        # Boucle à travers les lignes et les ajouter au PDF
        lines = courrier_contenu.split("\n")
        
        for line in lines:
            lignes_separees = []
            while len(line) > 95:
                index_espace = line[:95].rfind(' ')  # Recherche du dernier espace dans les 95 premiers caractères
                if index_espace != -1:
                    ligne_separee = line[:index_espace]  # Séparation à l'indice de l'espace
                    lignes_separees.append(ligne_separee)
                    line = line[index_espace+1:]  # La partie restante après l'espace
                else:
                    # Cas où il n'y a pas d'espace dans les 95 premiers caractères
                    ligne_separee = line[:95]
                    lignes_separees.append(ligne_separee)
                    line = line[95:]

            # Ajoute la dernière partie de la ligne (si elle existe) à la liste des lignes séparées
            if line:
                lignes_separees.append(line)

            # Affiche les lignes séparées
            for ligne in lignes_separees:
                c.drawString(x, y, ligne.strip())
                y -= 14  # Passer à la ligne suivante

                # Si nous atteignons le bas de la page, créer une nouvelle page
                if y < 50:
                    c.showPage()
                    y = 750

        # Enregistrer le PDF
        c.save()

    # Créer un fichier zip et ajouter les fichiers PDF générés
    zip_filename = os.path.join(output_folder, "pdf_files.zip")
    with zipfile.ZipFile(zip_filename, "w") as zip_file:
        for filename in filename_list:
            pdf_path = os.path.join(output_folder, filename)
            zip_file.write(pdf_path, os.path.basename(pdf_path))

    # Supprimer les fichiers PDF individuels après les avoir ajoutés au fichier zip
    for filename in filename_list:
        pdf_path = os.path.join(output_folder, filename)
        os.remove(pdf_path)


with open("./data/PAT_COURIER.txt", "r") as file:
    data = file.read()
    filename_list = generate_courrier_csv(data)
    generate_pdf(data, filename_list, "./courrier_files")
    
