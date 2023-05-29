import re
from tqdm import tqdm
fichier = 'test.txt'
nouveau_contenu = ""
numero = 1



with open(fichier, 'r') as f:
    contenu = f.read()

lignes = contenu.split('\n')


for ligne in tqdm(lignes):
  if "Nom;" in ligne:
    ligne_name_anonymise = re.sub(r'(?<=Nom;)[^\t]+',  str(numero).zfill(8), ligne)
    ligne_name_pre_anonymise = re.sub(r'(?<=NomPrenom;)[^\t]+',  str(numero).zfill(8), ligne_name_anonymise)
    ligne_anonymise = re.sub(r'(?<=NomVitale;)[^\t]+',  str(numero).zfill(8), ligne_name_pre_anonymise)
    if "QuickSearch;" in ligne:
       ligne_anonymise = re.sub(r'(?<=NomVitale;)[^\t]+',  str(numero).zfill(8), ligne_anonymise)
    nouveau_contenu += ligne_anonymise + '\n'
    numero += 1
  elif "NomPreAss;" in ligne:
    ligne_NomPreAss_anonymise = re.sub(r'(?<=NomPreAss;)[^\t]+',  str(numero).zfill(8), ligne)
    ligne_NoSS_anonymise = re.sub(r'(?<=NoSS;)[^\t]+',  str(numero).zfill(8), ligne_NomPreAss_anonymise)
    nouveau_contenu += ligne_NoSS_anonymise + '\n'
    numero += 1
  elif "Porteur;" in ligne:
    ligne_anonymise = re.sub(r'(?<=Porteur;)[^\t]+',  str(numero).zfill(8), ligne)
    nouveau_contenu += ligne_anonymise + '\n'
    numero += 1
  elif "PatientNom;" in ligne:
    ligne_anonymise = re.sub(r'(?<=PatientNom;)[^\t]+',  str(numero).zfill(8), ligne)
    nouveau_contenu += ligne_anonymise + '\n'
    numero += 1
  else:
      nouveau_contenu += ligne + '\n'
with open("test2.txt", 'w') as f:
    f.write(nouveau_contenu)
