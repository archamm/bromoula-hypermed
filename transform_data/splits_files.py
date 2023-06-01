import os
import re

def remove_newlines_except_before(input_filepath, output_filepath, target_string):
    pattern = r'\n(?!(?:.*' + re.escape(target_string) + '))'

    with open(input_filepath, 'r') as input_file:
        input_text = input_file.read()

    output_text = re.sub(pattern, '', input_text)

    with open(output_filepath, 'w') as output_file:
        output_file.write(output_text)

    print("Newlines removed and saved to", output_filepath)



# Prepare the output directory.
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

filename = "HMEXPASCII_anon.hmd"

output_file = None
output_filename = None
with open(filename, 'r') as file:
    for line in file:
        start_match = re.search(r"Export de la table : (\w+)", line)
        end_match = re.search(r"Fin de l' export de la table : (\w+)", line)

        # Check if we encountered a section start line.
        if start_match:
            # Close the current output file if it's open.
            if output_file is not None:
                output_file.close()
            # Open a new output file.
            section_name = start_match.group(1)
            output_filename = os.path.join(output_dir, f"{section_name}.txt")
            output_file = open(output_filename, 'w')
            continue  # Skip this line

        # Check if we encountered a section end line.
        if end_match:
            # Close the current output file.
            if output_file is not None:
                output_file.close()
                output_file = None
            continue  # Skip this line

        # Write the current line to the output file if it's open.
        if output_file is not None:
            output_file.write(line)

# Close the last output file if it's open.
if output_file is not None:
    output_file.close()


input_file = 'data/PAT_Criteres.txt'
output_file = 'data/PAT_Criteres_2.txt'
target = 'NumCons;'

remove_newlines_except_before(input_file, output_file, target)

input_file = 'data/MAQ_Criteres.txt'
output_file = 'data/MAQ_Criteres_2.txt'
target = 'CodeCrit;'

remove_newlines_except_before(input_file, output_file, target)
