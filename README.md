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
   python download_dataset_files.py --dataset-id 13738 --auth-token *****== --output-dir ./downloads
   ```

2. Convert downloaded Parquet files to CSV:
   ```bash
   python parquet_to_csv.py ./downloads/13738
   ```

#### 3. Field Name and Description Updater (`update_field_descriptions.py`)

This script allows you to update the field names and descriptions for a dataset in the Narrative API based on the data provided in a CSV file. It verifies that all fields in the CSV exist in the dataset schema before making any updates.

##### CSV File Structure

The CSV file should have the following two columns:

- **`field_name`**: The unique name of the field within the dataset.
- **`description`**: The new description to be applied to the field.

**Example CSV**:
```csv
field_name,description
field1,Description for field1
field2,Description for field2
```

##### Features
- **Schema Validation**: The script checks that all fields in the CSV exist in the dataset before updating.
- **Field Description Update**: Overwrites the existing field descriptions in the dataset with the values from the CSV.
- **Error Handling**: If any field in the CSV is not found in the dataset schema, the script will raise an error and abort.
- **API Integration**: Interacts with the Narrative API using the provided bearer authentication token.

##### Usage

1. **Command-Line Arguments**:
   - `api_token`: Your bearer authentication token for the Narrative API.
   - `dataset_id`: The ID of the dataset to update.
   - `csv_file_path`: The path to the CSV file containing field names and descriptions.

2. **Running the Script**:

```bash
python update_dataset.py <api_token> <dataset_id> <csv_file_path>
```

**Example**:
```bash
python update_dataset.py Q58rYVwyIu7SYGKGokc4GA== 13973 ./field_descriptions.csv
```

##### Workflow Example

1. Prepare your CSV file with updated field descriptions:
   ```csv
   field_name,description
   field1,Updated description for field1
   field2,Updated description for field2
   ```

2. Run the script to update the dataset:
   ```bash
   python update_dataset.py Q58rYVwyIu7SYGKGokc4GA== 13973 ./field_descriptions.csv
   ```

3. The script will verify the field names and update their descriptions in the Narrative dataset. It will output the success message along with the list of updated fields.

##### Error Handling

- If any fields in the CSV do not match the dataset schema, the script will show an error message and abort the operation.
- The script will also handle API errors such as authentication failures or issues with dataset retrieval.

#### 4. File Uploader with Chunking (`upload_file_to_dataset.py`)

This script uploads files to a Narrative dataset, supporting chunking for large files. It can handle CSV, JSON, and Parquet file formats.

##### Features
- **File Chunking**: Splits large files into chunks of 500,000 rows for upload.
- **Multiple File Formats**: Supports CSV, JSON, and Parquet.
- **API Integration**: Uses the Narrative API for uploading files in chunks.

##### Requirements

- **Python 3.x**
- **Required Libraries**: 
  ```bash
  pip install requests pandas pyarrow
  ```

##### Usage

1. **Command-Line Arguments**:
   - `api_token`: Your bearer authentication token for the Narrative API.
   - `dataset_id`: The ID of the dataset to upload the file to.
   - `file_path`: The path to the file to upload.
   - `file_type`: The type of the file to upload (`csv`, `json`, `parquet`).

2. **Running the Script**:

```bash
python upload_file_to_dataset.py <api_token> <dataset_id> <file_path> <file_type>
```

**Example**:
```bash
python upload_file_to_dataset.py Q58rYVwyIu7SYGKGokc4GA== 14016 ./data.csv csv
```

##### Workflow Example

1. Prepare your file for upload, ensuring it is in CSV, JSON, or Parquet format.

2. Run the script to upload the file in chunks:
   ```bash
   python upload_file_to_dataset.py Q58rYVwyIu7SYGKGokc4GA== 14016 ./data.csv csv
   ```

3. The script will split the file into chunks and upload each chunk to the specified dataset.

### Troubleshooting

If you encounter any issues:
1. Ensure all required libraries are installed
2. Verify your authentication token is valid
3. Check that you have write permissions in the output directory
4. For large datasets, ensure sufficient disk space is available

### Contributing

Feel free to submit issues and pull requests for improvements to these tools.
