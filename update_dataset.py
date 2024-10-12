import requests
import pandas as pd
import json
import argparse

def get_dataset(api_token, dataset_id):
    """Retrieve dataset by ID from the Narrative API."""
    url = f'https://app.narrative.io/openapi/datasets/{dataset_id}'
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json',
        'content-type': 'application/json',
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Error fetching dataset: {response.status_code} - {response.text}")
    
    return response.json()

def update_dataset(api_token, dataset_id, updated_dataset):
    """Update the dataset using the Narrative API."""
    url = f'https://app.narrative.io/openapi/datasets/{dataset_id}'
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json',
        'content-type': 'application/json',
    }
    response = requests.put(url, headers=headers, data=json.dumps(updated_dataset))
    
    if response.status_code != 200:
        raise Exception(f"Error updating dataset: {response.status_code} - {response.text}")
    
    print("Dataset successfully updated.")

def match_fields(csv_fields, dataset_fields):
    """Match CSV fields with dataset fields."""
    missing_fields = []
    for field_name in csv_fields:
        if field_name not in dataset_fields:
            missing_fields.append(field_name)
    return missing_fields

def update_field_descriptions(dataset_json, csv_df):
    """Update the dataset JSON with descriptions from the CSV."""
    updated_fields = []
    for _, row in csv_df.iterrows():
        field_name = row['field_name']
        field_description = row['description']
        
        # Check if the field exists in the dataset
        if field_name in dataset_json['schema']['properties']:
            dataset_json['schema']['properties'][field_name]['description'] = field_description
            updated_fields.append(field_name)
    
    return dataset_json, updated_fields

def main(api_token, dataset_id, csv_file_path):
    # Load the CSV
    csv_df = pd.read_csv(csv_file_path)
    csv_fields = csv_df['field_name'].tolist()
    
    # Step 1: Retrieve the dataset from the API
    dataset_json = get_dataset(api_token, dataset_id)
    
    # Step 2: Match fields
    dataset_fields = list(dataset_json['schema']['properties'].keys())
    missing_fields = match_fields(csv_fields, dataset_fields)
    
    if missing_fields:
        print(f"Error: The following fields are missing from the dataset: {missing_fields}")
        return
    
    # Step 3: Update the dataset JSON with the CSV descriptions
    updated_dataset, updated_fields = update_field_descriptions(dataset_json, csv_df)
    
    # Step 4: PUT the updated dataset back to the API
    update_dataset(api_token, dataset_id, updated_dataset)
    
    # Report success and updated fields
    print(f"Successfully updated the following fields: {updated_fields}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update a dataset's field descriptions using a CSV file.")
    
    # Adding arguments for the API token, dataset ID, and CSV file path
    parser.add_argument('api_token', type=str, help='Bearer auth token for Narrative API')
    parser.add_argument('dataset_id', type=str, help='ID of the dataset to update')
    parser.add_argument('csv_file_path', type=str, help='Path to the CSV file containing field names and descriptions')
    
    # Parse the arguments from the CLI
    args = parser.parse_args()
    
    # Call the main function with the parsed arguments
    main(args.api_token, args.dataset_id, args.csv_file_path)


