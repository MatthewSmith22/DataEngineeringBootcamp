import sys
from pathlib import Path


def analyze_file(file_path):
    """
    Analyze a single log file for error messages.
    
    Args:
        file_path: Path to the log file
        
    Returns:
        tuple: (error_count, list_of_error_messages)
    """
    error_count = 0
    error_messages = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Check if line contains ERROR
                if 'ERROR' in line:
                    error_count += 1
                    error_messages.append(line.strip())
    except Exception as e:
        print(f"Warning: Could not read file {file_path}: {e}", file=sys.stderr)
        return 0, []
    
    return error_count, error_messages


def main():
    """Main entry point for the log analyzer."""
    if len(sys.argv) != 2:
        print("Usage: python3 log_analyzer.py <log_directory>")
        print("Example: python3 log_analyzer.py /Users/myuser/airflow/logs/marketvol")
        sys.exit(1)
    
    log_directory = sys.argv[1]
    log_dir = Path(log_directory)
    
    # Check if directory exists
    if not log_dir.exists():
        print(f"Error: Directory {log_directory} does not exist")
        sys.exit(1)
    
    if not log_dir.is_dir():
        print(f"Error: {log_directory} is not a directory")
        sys.exit(1)
    
    # Get all .log files recursively
    file_list = list(log_dir.rglob('*.log'))
    
    if not file_list:
        print(f"No log files found in {log_directory}")
        sys.exit(0)
    
    # Initialize counters
    total_errors = 0
    all_error_messages = []
    
    # Analyze each file using the analyze_file function
    for log_file in file_list:
        count, cur_list = analyze_file(log_file)
        total_errors += count
        all_error_messages.extend(cur_list)
    
    # Print cumulative results
    print(f"Total number of errors: {total_errors}")
    
    if total_errors > 0:
        print("Here are all the errors:\n")
        for error_msg in all_error_messages:
            print(error_msg)


if __name__ == "__main__":
    main()