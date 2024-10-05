# nio_dataset_downloader
Downloads dataset part files in bulk from the Narrative API. 

## Download Dataset Files Script

This Python script downloads all Parquet files for a given dataset ID from the Narrative.io API. It handles pagination and authentication, allowing you to retrieve all files associated with a dataset efficiently.

### Features

- **Pagination Handling**: Automatically processes paginated API responses (1000 files per page).
- **Flexible Configuration**: Parameters can be set within the script or overridden via command-line arguments.
- **File Management**: Saves downloaded files in the specified output directory, preserving the original file paths.

### Requirements

- **Python 3.x**
- **Requests Library**: Install using `pip install requests`

### Usage

#### 1. Setting Parameters

You can configure the script by setting default values within the script or by providing command-line arguments.

- **Within the Script**:
  ```python
  # In download_dataset_files.py
  DEFAULT_DATASET_ID = 'your_dataset_id'
  DEFAULT_AUTH_TOKEN = 'your_auth_token'
  DEFAULT_OUTPUT_DIR = '.'
  ```

- **Via Command-Line Arguments**:
  - `--dataset-id`: Your dataset ID.
  - `--auth-token`: Your Bearer authentication token.
  - `--output-dir`: Directory to save downloaded files (default is the current directory).

#### 2. Running the Script

- **Using Script Defaults**:
  ```bash
  python download_dataset_files.py
  ```

- **Overriding with Command-Line Arguments**:
  ```bash
  python download_dataset_files.py --dataset-id <dataset_id> --auth-token <auth_token> --output-dir <output_directory>
  ```

**Example**:
```bash
python download_dataset_files.py --dataset-id 13738 --auth-token C3w9vSJf1WieKGli8uThew== --output-dir ./downloads
```


### Dependencies

Ensure you have the required libraries installed:

```bash
pip install requests
```
