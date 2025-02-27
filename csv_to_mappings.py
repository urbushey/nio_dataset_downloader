"""
Run a db query like the following:

SELECT 
    distinct attribute_id, mapping 
FROM mappings 
WHERE dataset_id IN 
    (123)

"""


import csv
import json

def clean_mapping(mapping_str):
    """Clean and parse the mapping JSON string from CSV"""
    mapping_dict = json.loads(mapping_str)
    
    # Remove the "dialect" entries from expressions
    if mapping_dict["type"] == "value_mapping":
        if isinstance(mapping_dict["expression"], dict):
            mapping_dict["expression"] = mapping_dict["expression"]["value"]
    elif mapping_dict["type"] == "object_mapping":
        for prop in mapping_dict["property_mappings"]:
            if isinstance(prop["expression"], dict):
                prop["expression"] = prop["expression"]["value"]
    
    return mapping_dict

def create_mapping_entry(attribute_id, mapping_str):
    """Create a minimal mapping entry with only required fields"""
    return {
        "attribute_id": int(attribute_id),
        "mapping": clean_mapping(mapping_str)
    }

def convert_csv_to_mappings(csv_file, output_file):
    """Convert CSV file to mappings JSON file"""
    mappings = []
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping_entry = create_mapping_entry(
                row['attribute_id'],
                row['mapping']
            )
            mappings.append(mapping_entry)
    
    with open(output_file, 'w') as f:
        json.dump(mappings, f, indent=2)
    
    print(f"Successfully converted {len(mappings)} mappings to {output_file}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert CSV mappings file to JSON format")
    parser.add_argument("csv_file", help="Input CSV file containing mappings")
    parser.add_argument("output_file", help="Output JSON file path")
    
    args = parser.parse_args()
    
    convert_csv_to_mappings(args.csv_file, args.output_file) 
