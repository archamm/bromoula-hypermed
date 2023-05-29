import os
import re

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

