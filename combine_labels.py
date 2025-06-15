import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

def load_labels(labels_file):
    """Load the anomaly labels from JSON file"""
    with open(labels_file, 'r') as f:
        labels = json.load(f)
    return labels

def process_csv_file(csv_path, anomaly_timestamps):
    """
    Process a single CSV file and add anomaly column
    
    Args:
        csv_path: Path to the CSV file
        anomaly_timestamps: List of anomaly timestamps for this file
    
    Returns:
        DataFrame with added anomaly column
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Initialize anomaly column with 0 (normal)
    df['anomaly'] = 0
    
    # Convert anomaly timestamps to datetime objects
    if anomaly_timestamps:
        anomaly_times = [pd.to_datetime(ts) for ts in anomaly_timestamps]
        
        # Mark anomaly points as 1
        df.loc[df['timestamp'].isin(anomaly_times), 'anomaly'] = 1
    
    return df

def combine_datasets_with_labels(data_dir, labels_file, output_dir=None):
    """
    Combine all datasets with their corresponding labels
    
    Args:
        data_dir: Directory containing the CSV files
        labels_file: JSON file containing anomaly labels
        output_dir: Directory to save processed files (optional)
    """
    # Load labels
    print("Loading labels...")
    labels = load_labels(labels_file)
    
    # Create output directory if specified
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    processed_count = 0
    error_count = 0
    
    # Process each file mentioned in labels
    for file_path, anomaly_timestamps in labels.items():
        try:
            # Construct full path to CSV file
            full_csv_path = os.path.join(data_dir, file_path)
            
            if not os.path.exists(full_csv_path):
                print(f"Warning: File not found: {full_csv_path}")
                error_count += 1
                continue
            
            print(f"Processing: {file_path}")
            
            # Process the CSV file
            df = process_csv_file(full_csv_path, anomaly_timestamps)
            
            # Print some statistics
            total_points = len(df)
            anomaly_points = df['anomaly'].sum()
            print(f"  - Total points: {total_points}")
            print(f"  - Anomaly points: {anomaly_points}")
            print(f"  - Normal points: {total_points - anomaly_points}")
            
            # Save the processed file
            if output_dir:
                # Create subdirectories in output if they don't exist
                output_file_path = os.path.join(output_dir, file_path)
                output_file_dir = os.path.dirname(output_file_path)
                os.makedirs(output_file_dir, exist_ok=True)
                df.to_csv(output_file_path, index=False)
                print(f"  - Saved to: {output_file_path}")
            else:
                # Overwrite original file
                df.to_csv(full_csv_path, index=False)
                print(f"  - Updated original file")
            
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            error_count += 1
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed: {processed_count} files")
    print(f"Errors encountered: {error_count} files")

def main():
    # Define paths
    data_directory = r"C:\Users\Kai\Documents\Time_Series_Anomaly_Detection\Dataset_Viz_Wrong\data"
    labels_file = r"C:\Users\Kai\Documents\Time_Series_Anomaly_Detection\Dataset_Viz_Wrong\labels\combined_labels.json"
    
    # Option 1: Process files in place (overwrite original files)
    print("=== Processing datasets with anomaly labels ===")
    print(f"Data directory: {data_directory}")
    print(f"Labels file: {labels_file}")
    print()
    
    # Check if files exist
    if not os.path.exists(data_directory):
        print(f"Error: Data directory not found: {data_directory}")
        return
    
    if not os.path.exists(labels_file):
        print(f"Error: Labels file not found: {labels_file}")
        return
    
    # Ask user if they want to create output directory or overwrite originals
    choice = input("Choose an option:\n1. Overwrite original files\n2. Create processed files in new directory\nEnter choice (1 or 2): ")
    
    if choice == "2":
        output_directory = r"C:\Users\Kai\Documents\Time_Series_Anomaly_Detection\Dataset_Viz_Wrong\data_with_labels"
        print(f"Creating output directory: {output_directory}")
        combine_datasets_with_labels(data_directory, labels_file, output_directory)
    else:
        print("Processing files in place (original files will be modified)...")
        combine_datasets_with_labels(data_directory, labels_file)

if __name__ == "__main__":
    main()
