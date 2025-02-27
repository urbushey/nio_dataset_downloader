# copy_mappings.py

import requests
import argparse
import json
import os

def get_dataset(dataset_id, token):
    url = f"https://api.narrative.io/datasets/{dataset_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def post_mapping(target_ds, company_id, mapping, token, is_admin=False):
    if is_admin:
        url = "https://api.narrative.io/mappings/"
    else:
        url = f"https://api.narrative.io/mappings/companies/{company_id}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {
        "dataset_id": target_ds,
        "attribute_id": mapping["attribute_id"],
        "mapping": mapping["mapping"]
    }
    response = requests.post(url, headers=headers, json=data)
    return response

def save_mappings_to_file(dataset_id, mappings):
    filename = f"{dataset_id}_mappings.json"
    with open(filename, 'w') as f:
        json.dump(mappings, f, indent=2)
    print(f"\nMappings saved to {filename}")

def load_mappings_from_file(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Mappings file '{filename}' not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Mappings file '{filename}' is not valid JSON")
        return None

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Copy mappings from source dataset to target dataset")
    parser.add_argument("--source_ds", type=str, required=False, help="ID of the source dataset")
    parser.add_argument("--target_ds", type=str, required=False, help="ID of the target dataset (optional)")
    parser.add_argument("--source_api_token", type=str, required=False, help="Bearer token for API authentication to fetch datasets")
    parser.add_argument("--target_api_token", type=str, required=False, help="Bearer token for API authentication to post mappings (required if target_ds is specified)")
    parser.add_argument("--admin", action="store_true", help="Use admin API endpoint for posting mappings")
    parser.add_argument("--mappings_file", type=str, help="JSON file containing mappings to load (required if source_ds not specified)")
    args = parser.parse_args()

    source_ds = args.source_ds
    target_ds = args.target_ds
    source_api_token = args.source_api_token
    target_api_token = args.target_api_token
    is_admin = args.admin
    mappings_file = args.mappings_file

    # Validate arguments
    if not source_ds and not mappings_file:
        parser.error("Either --source_ds or --mappings_file must be specified")
    
    if source_ds and not source_api_token:
        parser.error("--source_api_token is required when --source_ds is specified")

    if target_ds and not target_api_token:
        parser.error("--target_api_token is required when --target_ds is specified")

    if not source_ds and not target_ds:
        parser.error("At least one of --source_ds or --target_ds must be specified")

    # Get mappings either from source dataset or file
    if source_ds:
        # Retrieve source dataset
        source_data = get_dataset(source_ds, source_api_token)
        mappings = source_data.get("mappings", [])
        # Save mappings to file
        save_mappings_to_file(source_ds, mappings)
    else:
        # Load mappings from file
        mappings = load_mappings_from_file(mappings_file)
        if not mappings:
            return

    # If no target dataset specified, exit after saving/loading
    if not target_ds:
        print("No target dataset specified. Mappings have been saved to file only.")
        return

    company_id = None
    if not is_admin:
        # Get target dataset to retrieve company_id
        try:
            target_data = get_dataset(target_ds, target_api_token)
            company_id = target_data.get("company_id")
            if not company_id:
                print("Error: Could not find company_id in target dataset")
                return
        except Exception as e:
            print(f"Error fetching target dataset: {str(e)}")
            return

    # Copy mappings to target dataset
    success_count = 0
    failure_count = 0
    failures = []

    for mapping in mappings:
        response = post_mapping(target_ds, company_id, mapping, target_api_token, is_admin)
        
        if response.status_code == 200:
            print(f"Mapping for attribute ID {mapping['attribute_id']} successfully posted.")
            success_count += 1
        else:
            print(f"Failed to post mapping for attribute ID {mapping['attribute_id']}: {response.text}")
            failure_count += 1
            failures.append({"attribute_id": mapping['attribute_id'], "error": response.text})

    # Summary
    print("\nCopy Summary:")
    print(f"Total successful mappings: {success_count}")
    print(f"Total failed mappings: {failure_count}")
    if failures:
        print("\nFailed Mappings Details:")
        for failure in failures:
            print(f"Attribute ID: {failure['attribute_id']}, Error: {failure['error']}")

if __name__ == "__main__":
    main()
