import argparse
import os
import pandas as pd
from pathlib import Path

def process_parquet_files(input_path, output_filename=None):
    # Convert input path to Path object
    path = Path(input_path).resolve()
    
    # Use the last part of the path as the default output filename if none provided
    if output_filename is None:
        output_filename = f"{path.name}.csv"
    
    # Ensure output filename ends with .csv
    if not output_filename.endswith('.csv'):
        output_filename += '.csv'
    
    # Get all parquet files in the directory (non-recursive)
    parquet_files = list(path.glob('*.parquet'))
    
    if not parquet_files:
        print(f"No parquet files found in {path}")
        return
    
    # Initialize empty DataFrame to store all data
    all_data = pd.DataFrame()
    
    # Process each parquet file
    for i, file in enumerate(parquet_files, 1):
        print(f"Processing file {i}/{len(parquet_files)}: {file.name}")
        df = pd.read_parquet(file)
        all_data = pd.concat([all_data, df], ignore_index=True)
    
    # Write to CSV
    print(f"Writing data to {output_filename}")
    all_data.to_csv(output_filename, index=False)
    print(f"Successfully wrote {len(all_data)} rows to {output_filename}")

def main():
    parser = argparse.ArgumentParser(description='Convert Parquet files in a directory to a single CSV file.')
    parser.add_argument('path', type=str, help='Path to the directory containing parquet files')
    parser.add_argument('-o', '--output', type=str, help='Output CSV filename (optional)', default=None)
    
    args = parser.parse_args()
    
    process_parquet_files(args.path, args.output)

if __name__ == "__main__":
    main()
