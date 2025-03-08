import os

import xml.etree.ElementTree as ET


def split_jmdict(input_file, output_dir, entries_per_file=1000):
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Parse the XML file
    tree = ET.parse(input_file)
    root = tree.getroot()

    # Get all entries
    entries = root.findall('entry')
    total_entries = len(entries)

    # Calculate number of files needed
    num_files = (total_entries + entries_per_file - 1) // entries_per_file

    for i in range(num_files):
        # Create a new root element with the same tag and attributes
        new_root = ET.Element(root.tag, root.attrib)

        # Calculate start and end indices for current chunk
        start = i * entries_per_file
        end = min((i + 1) * entries_per_file, total_entries)

        # Add entries to new root
        for entry in entries[start:end]:
            new_root.append(entry)

        # Create new tree and write to file
        new_tree = ET.ElementTree(new_root)
        output_file = os.path.join(output_dir, f'jmdict_part_{i+1}.xml')

        # Write the XML file with proper encoding and declaration
        new_tree.write(output_file, encoding='utf-8', xml_declaration=True)

        print(f'Created file {output_file} with {end-start} entries')


# Example usage:
if __name__ == "__main__":
    split_jmdict('JMdict.xml', 'jmdict_parts', entries_per_file=1000)
