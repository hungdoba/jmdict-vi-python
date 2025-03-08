import os
from pathlib import Path

import xml.etree.ElementTree as ET


def merge_xml_files(input_folder: str, output_file: str) -> None:
    """
    Merge multiple XML files from input folder into one JMdict.xml file

    Args:
        input_folder (str): Path to folder containing XML parts
        output_file (str): Path to output merged XML file
    """
    # Create root element for merged file
    merged_root = ET.Element('JMdict')

    # Process each XML file in the input folder
    for xml_file in Path(input_folder).glob('*.xml'):
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Add all entry elements to merged root
            for entry in root.findall('entry'):
                merged_root.append(entry)

        except ET.ParseError as e:
            print(f"Error parsing {xml_file}: {e}")
            continue

    # Create the final XML tree
    merged_tree = ET.ElementTree(merged_root)

    # Write the merged XML with proper declaration
    with open(output_file, 'wb') as f:
        f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
        merged_tree.write(f, encoding='utf-8')


if __name__ == '__main__':
    # Example usage
    input_folder = "jmdict_vi"
    output_file = "jmdict.xml"
    merge_xml_files(input_folder, output_file)
