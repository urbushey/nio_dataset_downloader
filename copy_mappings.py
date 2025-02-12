# copy_mappings.py

import requests
import argparse

def get_dataset(dataset_id, token):
    url = f"https://api.narrative.io/datasets/{dataset_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def post_mapping(target_ds, mapping, token):
    url = "https://api.narrative.io/mappings/"
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

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Copy mappings from source dataset to target dataset")
    parser.add_argument("source_ds", type=str, help="ID of the source dataset")
    parser.add_argument("target_ds", type=str, help="ID of the target dataset")
    parser.add_argument("source_api_token", type=str, help="Bearer token for API authentication to fetch datasets")
    parser.add_argument("admin_api_token", type=str, help="Bearer token for API authentication to post mappings")
    args = parser.parse_args()

    source_ds = args.source_ds
    target_ds = args.target_ds
    source_api_token = args.source_api_token
    admin_api_token = args.admin_api_token

    # Retrieve both datasets
    source_data = get_dataset(source_ds, source_api_token)

    success_count = 0
    failure_count = 0
    failures = []

    # Loop through each mapping in the source dataset
    for mapping in source_data.get("mappings", []):
        response = post_mapping(target_ds, mapping, admin_api_token)
        
        if response.status_code == 200:
            print(f"Mapping ID {mapping['id']} successfully posted.")
            success_count += 1
        else:
            print(f"Failed to post mapping ID {mapping['id']}: {response.text}")
            failure_count += 1
            failures.append({"mapping_id": mapping['id'], "error": response.text})

    # Summary
    print("\nSummary:")
    print(f"Total successful mappings: {success_count}")
    print(f"Total failed mappings: {failure_count}")
    if failures:
        print("\nFailed Mappings Details:")
        for failure in failures:
            print(f"Mapping ID: {failure['mapping_id']}, Error: {failure['error']}")

if __name__ == "__main__":
    main()
