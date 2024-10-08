# nio_dataset_downloader
Downloads dataset part files in bulk from the Narrative API and provides tools for processing the downloaded data.

## Download Dataset Files Script

This Python script downloads all Parquet files for a given dataset ID from the Narrative.io API. It handles pagination and authentication, allowing you to retrieve all files associated with a dataset efficiently.

### Features

- **Pagination Handling**: Automatically processes paginated API responses (1000 files per page).
- **Flexible Configuration**: Parameters can be set within the script or overridden via command-line arguments.
- **File Management**: Saves downloaded files in the specified output directory, preserving the original file paths.

### Requirements

- **Python 3.x**
- **Required Libraries**: 
  ```bash
  pip install requests pandas pyarrow
  ```

### Scripts

#### 1. Dataset Downloader (`download_dataset_files.py`)

##### Setting Parameters

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

##### Running the Script

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

#### 2. Parquet to CSV Converter (`parquet_to_csv.py`)

This utility script combines multiple Parquet files from a directory into a single CSV file.

##### Features
- Processes all Parquet files in a specified directory (non-recursive)
- Combines them into a single CSV file
- Default output filename based on input directory name
- Optional custom output filename
- Progress reporting during processing

##### Usage

- **Basic Usage** (uses directory name for output):
  ```bash
  python parquet_to_csv.py ./datasets/123
  ```
  This will create `123.csv` containing data from all Parquet files in `./datasets/123/`

- **Custom Output Filename**:
  ```bash
  python parquet_to_csv.py ./datasets/123 -o custom_output.csv
  ```

##### Command-Line Arguments
- `path` (required): Path to the directory containing Parquet files
- `-o, --output` (optional): Custom output filename for the CSV

##### Example Workflow
1. Download dataset:
   ```bash
   python download_dataset_files.py --dataset-id 13738 --output-dir ./downloads
   ```

2. Convert downloaded Parquet files to CSV:
   ```bash
   python parquet_to_csv.py ./downloads/13738
   ```

### Troubleshooting

If you encounter any issues:
1. Ensure all required libraries are installed
2. Verify your authentication token is valid
3. Check that you have write permissions in the output directory
4. For large datasets, ensure sufficient disk space is available

### Contributing

Feel free to submit issues and pull requests for improvements to these tools.
