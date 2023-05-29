import re
from tqdm import tqdm

fichier = 'HMEXPASCII.hmd'
output = 'HMEXPASCII_anon.hmd'
numero = 1

with open(fichier, 'r') as f, open(output, 'w') as out_f:
    for ligne in tqdm(f):
        ligne = ligne.rstrip('\n')  # remove the trailing newline
        if "Nom;" in ligne:
            ligne = re.sub(r'(?<=Nom;)[^\t]+', str(numero).zfill(8), ligne)
            ligne = re.sub(r'(?<=NomPrenom;)[^\t]+', str(numero).zfill(8), ligne)
            ligne = re.sub(r'(?<=NomVitale;)[^\t]+', str(numero).zfill(8), ligne)
            if "QuickSearch;" in ligne:
                ligne = re.sub(r'(?<=QuickSearch;)[^\t]+', str(numero).zfill(8), ligne)
            numero += 1
        elif "NomPreAss;" in ligne:
            ligne = re.sub(r'(?<=NomPreAss;)[^\t]+', str(numero).zfill(8), ligne)
            ligne = re.sub(r'(?<=NoSS;)[^\t]+', str(numero).zfill(8), ligne)
            numero += 1
        elif "Porteur;" in ligne:
            ligne = re.sub(r'(?<=Porteur;)[^\t]+', str(numero).zfill(8), ligne)
            numero += 1
        elif "PatientNom;" in ligne:
            ligne = re.sub(r'(?<=PatientNom;)[^\t]+', str(numero).zfill(8), ligne)
            numero += 1
        out_f.write(ligne + '\n')
