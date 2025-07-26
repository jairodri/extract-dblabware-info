"""
HTTP Status Code Log Parser

Extracts HTTP status codes and timestamps from web service integration logs
and generates a CSV report with the results.

Author: Log Analysis Team
Version: 1.0
"""

import re
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import argparse


class HTTPLogParser:
    """
    Parser for extracting HTTP status codes from web service logs.
    
    Handles log entries with the format:
    ==={Received HTTP Header DD/MM/YYYY HH:MM:SS}====
    HTTP/1.1 100 Continue
    HTTP/1.1 XXX Status Message
    """
    
    def __init__(self, log_file_path: str):
        """
        Initialize the parser with a log file path.
        
        Args:
            log_file_path (str): Path to the log file to parse
        """
        self.log_file_path = Path(log_file_path)
        self.http_entries: List[Dict[str, str]] = []
        
        # Regex patterns for parsing
        self.header_pattern = re.compile(
            r'===\{Received HTTP Header (\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\}====',
            re.IGNORECASE
        )
        self.http_status_pattern = re.compile(
            r'HTTP/1\.1 (\d{3}) (.+)',
            re.IGNORECASE
        )
    
    def parse_log_file(self) -> List[Dict[str, str]]:
        """
        Parse the log file and extract HTTP status entries.
        
        Returns:
            List[Dict[str, str]]: List of dictionaries containing timestamp,
                                 status_code, and status_message
        
        Raises:
            FileNotFoundError: If the log file doesn't exist
            Exception: For other parsing errors
        """
        if not self.log_file_path.exists():
            raise FileNotFoundError(f"Log file not found: {self.log_file_path}")
        
        print(f"Parsing log file: {self.log_file_path}")
        
        try:
            with open(self.log_file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Split content by header markers to process each entry
            entries = self._split_by_headers(content)
            
            for entry in entries:
                parsed_entry = self._parse_entry(entry)
                if parsed_entry:
                    self.http_entries.append(parsed_entry)
            
            print(f"Found {len(self.http_entries)} HTTP status entries")
            return self.http_entries
            
        except Exception as e:
            print(f"Error parsing log file: {e}")
            raise
    
    def _split_by_headers(self, content: str) -> List[str]:
        """
        Split log content by HTTP header markers.
        
        Args:
            content (str): Full log file content
            
        Returns:
            List[str]: List of individual log entries
        """
        # Split by the header pattern and keep the headers
        parts = re.split(r'(===\{Received HTTP Header [^}]+\}====)', content)
        
        # Combine headers with their following content
        entries = []
        for i in range(1, len(parts), 2):  # Start from 1, step by 2 to get header+content pairs
            if i + 1 < len(parts):
                entry = parts[i] + parts[i + 1]
                entries.append(entry.strip())
        
        return entries
    
    def _parse_entry(self, entry: str) -> Optional[Dict[str, str]]:
        """
        Parse a single log entry to extract timestamp and HTTP status.
        
        Args:
            entry (str): Single log entry containing header and HTTP responses
            
        Returns:
            Optional[Dict[str, str]]: Parsed entry data or None if parsing fails
        """
        lines = entry.strip().split('\n')
        
        # Extract timestamp from header
        timestamp = self._extract_timestamp(lines[0])
        if not timestamp:
            return None
        
        # Find the final HTTP status (skip 100 Continue)
        final_status = self._extract_final_http_status(lines[1:])
        if not final_status:
            return None
        
        return {
            'timestamp': timestamp,
            'status_code': final_status['code'],
            'status_message': final_status['message']
        }
    
    def _extract_timestamp(self, header_line: str) -> Optional[str]:
        """
        Extract timestamp from the header line.
        
        Args:
            header_line (str): Header line containing timestamp
            
        Returns:
            Optional[str]: Extracted timestamp or None if not found
        """
        match = self.header_pattern.search(header_line)
        if match:
            return match.group(1)
        return None
    
    def _extract_final_http_status(self, http_lines: List[str]) -> Optional[Dict[str, str]]:
        """
        Extract the final HTTP status from response lines.
        
        Skips "100 Continue" responses and returns the final status.
        
        Args:
            http_lines (List[str]): Lines containing HTTP responses
            
        Returns:
            Optional[Dict[str, str]]: Dictionary with 'code' and 'message' or None
        """
        final_status = None
        
        for line in http_lines:
            line = line.strip()
            if not line:
                continue
                
            match = self.http_status_pattern.match(line)
            if match:
                status_code = match.group(1)
                status_message = match.group(2).strip()
                
                # Skip 100 Continue responses, keep the final one
                if status_code != '100':
                    final_status = {
                        'code': status_code,
                        'message': status_message
                    }
        
        return final_status
    
    def generate_csv_report(self, output_file: str = None) -> str:
        """
        Generate a CSV report with the extracted HTTP status data.
        
        Args:
            output_file (str, optional): Output CSV file path. If None, generates
                                       a name based on the input log file.
        
        Returns:
            str: Path to the generated CSV file
        """
        if not self.http_entries:
            print("No HTTP entries found. Run parse_log_file() first.")
            return ""
        
        # Generate output filename if not provided
        if output_file is None:
            output_file = self.log_file_path.stem + "_http_status_report.csv"
        
        output_path = Path(output_file)
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['timestamp', 'status_code', 'status_message', 'formatted_date']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data with formatted date
                for entry in self.http_entries:
                    # Add formatted date for better readability
                    formatted_date = self._format_timestamp(entry['timestamp'])
                    
                    writer.writerow({
                        'timestamp': entry['timestamp'],
                        'status_code': entry['status_code'],
                        'status_message': entry['status_message'],
                        'formatted_date': formatted_date
                    })
            
            print(f"CSV report generated: {output_path}")
            print(f"Total entries: {len(self.http_entries)}")
            
            return str(output_path)
            
        except Exception as e:
            print(f"Error generating CSV report: {e}")
            return ""
    
    def _format_timestamp(self, timestamp: str) -> str:
        """
        Format timestamp for better readability in CSV.
        
        Args:
            timestamp (str): Original timestamp string (DD/MM/YYYY HH:MM:SS)
            
        Returns:
            str: Formatted timestamp or original if parsing fails
        """
        try:
            # Parse the timestamp
            dt = datetime.strptime(timestamp, '%d/%m/%Y %H:%M:%S')
            # Return in ISO format for better sorting
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Return original if parsing fails
            return timestamp
    
    def get_status_summary(self) -> Dict[str, int]:
        """
        Get a summary of HTTP status codes and their frequencies.
        
        Returns:
            Dict[str, int]: Dictionary with status codes as keys and counts as values
        """
        status_counts = {}
        for entry in self.http_entries:
            status = f"{entry['status_code']} {entry['status_message']}"
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return status_counts
    
    def print_summary(self):
        """Print a summary of the parsed HTTP status codes."""
        if not self.http_entries:
            print("No entries to summarize.")
            return
        
        print("\n" + "="*50)
        print("HTTP STATUS CODE SUMMARY")
        print("="*50)
        
        status_summary = self.get_status_summary()
        
        # Sort by status code
        sorted_statuses = sorted(status_summary.items(), key=lambda x: x[0])
        
        total_entries = len(self.http_entries)
        
        for status, count in sorted_statuses:
            percentage = (count / total_entries) * 100
            print(f"{status:<30} {count:>6} ({percentage:>5.1f}%)")
        
        print("-" * 50)
        print(f"{'Total entries:':<30} {total_entries:>6}")
        print("="*50)


def main():
    """
    Main function to run the HTTP log parser from command line.
    """
    parser = argparse.ArgumentParser(
        description='Extract HTTP status codes from web service integration logs'
    )
    parser.add_argument(
        'log_file', 
        help='Path to the log file to parse'
    )
    parser.add_argument(
        '-o', '--output', 
        help='Output CSV file path (optional)', 
        default=None
    )
    parser.add_argument(
        '-s', '--summary', 
        action='store_true',
        help='Display summary of HTTP status codes'
    )
    
    args = parser.parse_args()
    
    try:
        # Create parser instance
        log_parser = HTTPLogParser(args.log_file)
        
        # Parse the log file
        log_parser.parse_log_file()
        
        # Generate CSV report
        output_file = log_parser.generate_csv_report(args.output)
        
        # Show summary if requested
        if args.summary:
            log_parser.print_summary()
        
        print(f"\nProcess completed successfully!")
        print(f"CSV report saved to: {output_file}")
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    
    return 0


# Example usage as a module
def parse_http_log(log_file_path: str, output_csv_path: str = None) -> Tuple[str, List[Dict[str, str]]]:
    """
    Convenience function to parse HTTP log and generate CSV report.
    
    Args:
        log_file_path (str): Path to the log file
        output_csv_path (str, optional): Path for the output CSV file
        
    Returns:
        Tuple[str, List[Dict[str, str]]]: CSV file path and list of parsed entries
    """
    parser = HTTPLogParser(log_file_path)
    entries = parser.parse_log_file()
    csv_path = parser.generate_csv_report(output_csv_path)
    return csv_path, entries


if __name__ == "__main__":
    exit(main())