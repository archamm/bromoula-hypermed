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

def generate_dataframe(input_data):
    numcons_list = get_num_cons_list(input_data)
    textcons_list = get_text_cons_list(input_data)    
    df = pd.DataFrame({'NumCons': numcons_list, 'TextCons_': textcons_list})
    return df

with open("data/PAT_TextCons.txt", "r") as file:
    data = file.read()
    df = generate_dataframe(data)
    df.to_csv("test.csv")

