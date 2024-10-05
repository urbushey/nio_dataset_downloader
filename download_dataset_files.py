import requests
import argparse
import os
import urllib.parse

# Default parameters (can be set here)
DEFAULT_DATASET_ID = 'your_dataset_id'
DEFAULT_AUTH_TOKEN = 'your_auth_token'
DEFAULT_OUTPUT_DIR = '.'

def main():
    parser = argparse.ArgumentParser(description='Download all Parquet files for a dataset.')
    parser.add_argument('--dataset-id', type=str, default=DEFAULT_DATASET_ID, help='Dataset ID')
    parser.add_argument('--auth-token', type=str, default=DEFAULT_AUTH_TOKEN, help='Bearer authentication token')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_OUTPUT_DIR, help='Directory to save downloaded files')
    args = parser.parse_args()

    dataset_id = args.dataset_id
    auth_token = args.auth_token
    output_dir = os.path.join(args.output_dir, args.dataset_id)

    if not dataset_id or not auth_token:
        print("Dataset ID and Auth Token must be provided either in the script or via command-line arguments.")
        return

    base_url = 'https://app.narrative.io/openapi'
    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {auth_token}',
        'content-type': 'application/json',
    }

    per_page = 1000
    has_next = True
    next_snapshot = None

    while has_next:
        params = {'per_page': per_page}
        if next_snapshot:
            params['snapshot'] = next_snapshot

        url = f'{base_url}/datasets/{dataset_id}/find-files'
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        files_per_snapshot = data.get('files_per_snapshot', [])
        has_next = data.get('has_next', False)
        next_snapshot = data.get('next_snapshot', None)

        for snapshot in files_per_snapshot:
            snapshot_id = snapshot['snapshot_id']
            files = snapshot['files']
            is_downloadable = snapshot.get('is_downloadable', False)

            if not is_downloadable:
                print(f"Snapshot {snapshot_id} is not downloadable.")
                continue

            for file_info in files:
                file_path = file_info['path']
                size = file_info['size']

                print(f"Processing file: {file_path} (size: {size} bytes)")

                # Get the download URL
                download_url = get_download_url(base_url, dataset_id, snapshot_id, file_path, headers)
                if not download_url:
                    print(f"Failed to get download URL for file {file_path}")
                    continue

                # Download the file
                download_file(download_url, file_path, output_dir)

def get_download_url(base_url, dataset_id, snapshot_id, file_path, headers):
    encoded_file_path = urllib.parse.quote(file_path, safe='')
    url = f'{base_url}/datasets/{dataset_id}/snapshots/{snapshot_id}/files-added/{encoded_file_path}/download'
    response = requests.post(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get download URL for {file_path}: {response.status_code}")
        return None
    data = response.json()
    return data.get('download_url')

def download_file(download_url, file_path, output_dir):
    response = requests.get(download_url, stream=True)
    if response.status_code != 200:
        print(f"Failed to download file {file_path}: {response.status_code}")
        return
    output_file_path = os.path.join(output_dir, file_path)
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    with open(output_file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f'Downloaded {file_path} to {output_file_path}')

if __name__ == '__main__':
    main()
