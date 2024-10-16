import requests
import argparse
import os
import pandas as pd
import logging
import tempfile

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

CHUNK_SIZE_ROWS = 50000000

def get_upload_url(api_token, file_name):
    """Get the upload URL from the Narrative API."""
    url = f'https://app-dev.narrative.io/openapi/uploads/{file_name}'
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json',
        'content-type': 'application/json',
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()

def upload_file_to_s3(upload_url, file_chunk):
    """Upload the file chunk to the S3 URL provided by the Narrative API."""
    response = requests.put(upload_url, data=file_chunk)
    response.raise_for_status()

def notify_narrative(api_token, dataset_id, source_file):
    url = f"https://app-dev.narrative.io/openapi/datasets/{dataset_id}/upload"
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'source_file': source_file  # Use the 'path' from upload_info
    }
    # Log the request details
    logging.debug(f"URL: {url}")
    logging.debug(f"Headers: {headers}")
    logging.debug(f"Payload: {payload}")
    
    response = requests.post(url, headers=headers, json=payload)
    
    # Log the response details
    logging.debug(f"Response Status Code: {response.status_code}")
    logging.debug(f"Response Content: {response.content.decode('utf-8')}")
    
    response.raise_for_status()



def chunk_file(file_path, file_type):
    """Yield chunks of the file based on size or row count."""
    if file_type == 'csv':
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE_ROWS):
            yield chunk.to_csv(index=False).encode('utf-8')
    elif file_type == 'json':
        df = pd.read_json(file_path, lines=True)
        for i in range(0, len(df), CHUNK_SIZE_ROWS):
            yield df.iloc[i:i + CHUNK_SIZE_ROWS].to_json(orient='records', lines=True).encode('utf-8')
    elif file_type == 'parquet':
        df = pd.read_parquet(file_path)
        for i in range(0, len(df), CHUNK_SIZE_ROWS):
            yield df.iloc[i:i + CHUNK_SIZE_ROWS].to_parquet(index=False)

def main(api_token, dataset_id, file_path, file_type):
    file_name = os.path.basename(file_path)
    
    # Step 1: Chunk the file and upload each chunk
    for i, file_chunk in enumerate(chunk_file(file_path, file_type)):
        chunk_file_name = f"{file_name}_part_{i}"
        
        # Create a temporary file to store the chunk
        with tempfile.NamedTemporaryFile(delete=False, suffix=chunk_file_name) as temp_file:
            temp_file.write(file_chunk)
            temp_file_path = temp_file.name
        
        try:
            # Get the upload URL for each chunk
            upload_info = get_upload_url(api_token, chunk_file_name)
            upload_url = upload_info['url']
            upload_path = upload_info['path']  # Extract the 'path' here
            
            # Upload the file chunk to the S3 URL
            upload_file_to_s3(upload_url, file_chunk)
            
            # Notify Narrative of the upload using the 'path' from upload_info
            notify_narrative(api_token, dataset_id, upload_path)
            
            print(f"Chunk {i} of {file_name} successfully uploaded to dataset {dataset_id}.")
        finally:
            # Ensure the temporary file is deleted after use
            os.remove(temp_file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a file to a Narrative dataset.")
    parser.add_argument('api_token', type=str, help='Bearer auth token for Narrative API')
    parser.add_argument('dataset_id', type=str, help='ID of the dataset to upload the file to')
    parser.add_argument('file_path', type=str, help='Path to the file to upload')
    parser.add_argument('file_type', type=str, choices=['csv', 'json', 'parquet'], help='Type of the file to upload (csv, json, parquet)')
    
    args = parser.parse_args()
    
    main(args.api_token, args.dataset_id, args.file_path, args.file_type)
