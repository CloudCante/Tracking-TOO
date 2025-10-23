import requests
import csv
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# API Configuration
API_BASE_URL = "http://10.23.8.215:5000/api/v1/sql-portal"

def convert_to_raw_timestamp(iso_timestamp: str) -> str:
    """
    Convert ISO timestamp to raw DB format
    First convert to UTC, then add 4 hours to match database values
    """
    if not iso_timestamp:
        return ''
    
    try:
        # Parse ISO format
        dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
        
        # Convert to UTC (subtract 8 hours from Taiwan time)
        utc_dt = dt.replace(tzinfo=None) + timedelta(hours=-8)
        
        # Add 4 hours to match database values
        adjusted_dt = utc_dt + timedelta(hours=4)
        
        return adjusted_dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, AttributeError):
        # If parsing fails, return original
        return iso_timestamp

class WebAllStationTimestampsExporter:
    def __init__(self, api_base_url: str = API_BASE_URL):
        self.api_base_url = api_base_url
        self.session = requests.Session()
    
    def get_serial_history(self, serial_numbers: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        Get production history for serial numbers from the API
        """
        url = f"{self.api_base_url}/serial-history"
        
        payload = {
            "serialNumbers": serial_numbers
        }
        
        if start_date and end_date:
            payload["startDate"] = start_date
            payload["endDate"] = end_date
        
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return {"success": False, "error": str(e)}
    
    def process_all_station_timestamps(self, serial_number: str, history_data: List[Dict]) -> Dict:
        """
        Process all station timestamps for a serial number (same logic as original script)
        """
        if not history_data:
            return {
                'serial_number': serial_number,
                'stations': {}
            }
        
        # Filter only workstation data (we need workstation_name, history_station_start_time, history_station_end_time)
        workstation_data = [
            record for record in history_data 
            if record.get('source') == 'workstation' and record.get('workstation_name')
        ]
        
        if not workstation_data:
            return {
                'serial_number': serial_number,
                'stations': {}
            }
        
        # Build dictionary of stations (with lists for multiple visits)
        stations = {}
        for record in workstation_data:
            station = record['workstation_name']
            start_time = record.get('history_station_start_time')
            end_time = record.get('history_station_end_time')
            
            if station not in stations:
                stations[station] = []
            stations[station].append({
                'start': start_time,
                'end': end_time
            })
        
        # Use MOST RECENT visit for each station
        result = {
            'serial_number': serial_number,
            'stations': {}
        }
        
        for station_name, visits in stations.items():
            # Get the last (most recent) visit
            last_visit = visits[-1]
            result['stations'][station_name] = {
                'start': last_visit['start'],
                'end': last_visit['end']
            }
        
        return result
    
    def export_all_station_timestamps(self, serial_numbers: List[str], output_file: str = "all_station_timestamps.csv", start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Export all station timestamps for multiple serial numbers
        """
        print(f"Processing {len(serial_numbers)} serial numbers...")
        
        results = []
        
        # Process serial numbers in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, len(serial_numbers), batch_size):
            batch = serial_numbers[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(serial_numbers) + batch_size - 1)//batch_size}...")
            
            # Get history data for this batch
            api_response = self.get_serial_history(batch, start_date, end_date)
            
            if not api_response.get('success'):
                print(f"API error for batch: {api_response.get('error', 'Unknown error')}")
                continue
            
            # Group history data by serial number
            history_by_sn = {}
            for record in api_response.get('history', []):
                sn = record['sn']
                if sn not in history_by_sn:
                    history_by_sn[sn] = []
                history_by_sn[sn].append(record)
            
            # Process each serial number in the batch
            for sn in batch:
                if i % 100 == 0 and i > 0:
                    print(f"Processed {i}/{len(serial_numbers)}...")
                
                history_data = history_by_sn.get(sn, [])
                result = self.process_all_station_timestamps(sn, history_data)
                results.append(result)
        
        # Get ALL unique stations from all results
        all_stations = set()
        for result in results:
            all_stations.update(result['stations'].keys())
        
        # Sort them alphabetically for consistent ordering
        all_stations = sorted(all_stations)
        
        print(f"\n✓ Found {len(all_stations)} unique stations across all serial numbers:")
        for station in all_stations:
            print(f"  - {station}")
        
        # Create CSV with all station timestamps
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Build header with start/end columns for each station
            header = ['Serial Number']
            for station in all_stations:
                header.append(f'{station} Start Time')
                header.append(f'{station} End Time')
            writer.writerow(header)
            
            # Write data rows
            for result in results:
                row = [result['serial_number']]
                stations = result['stations']
                
                for station in all_stations:
                    if station in stations:
                        start = stations[station]['start']
                        end = stations[station]['end']
                        row.append(convert_to_raw_timestamp(start) if start else '')
                        row.append(convert_to_raw_timestamp(end) if end else '')
                    else:
                        row.append('')  # No start time
                        row.append('')  # No end time
                
                writer.writerow(row)
        
        print(f"\n✓ All station timestamps exported to {output_file}")
        
        # Show a sample
        if results:
            print("\n" + "="*80)
            print("Sample data for first serial number:")
            print("="*80)
            sample = results[0]
            print(f"Serial: {sample['serial_number']}")
            for station, times in sorted(sample['stations'].items()):
                print(f"  {station:20} Start: {times['start']}  |  End: {times['end']}")
        
        return results

def main():
    """
    Main function - can be used in different ways:
    1. Command line with serial numbers
    2. Read from CSV file
    3. Interactive input
    """
    import sys
    
    exporter = WebAllStationTimestampsExporter()
    
    if len(sys.argv) > 1:
        # Command line usage: python all_stations_time.py SN123 SN456 SN789
        serial_numbers = sys.argv[1:]
        exporter.export_all_station_timestamps(serial_numbers)
    else:
        # Interactive mode or CSV file
        print("Web-based All Station Timestamps Exporter")
        print("=" * 40)
        print("1. Enter serial numbers manually")
        print("2. Read from CSV file")
        print("3. Read from numbers.csv (default)")
        
        choice = input("\nChoose option (1/2/3): ").strip()
        
        if choice == "1":
            print("\nEnter serial numbers (one per line, empty line to finish):")
            serial_numbers = []
            while True:
                sn = input().strip()
                if not sn:
                    break
                serial_numbers.append(sn)
            
            if serial_numbers:
                exporter.export_all_station_timestamps(serial_numbers)
            else:
                print("No serial numbers entered.")
        
        elif choice == "2":
            filename = input("Enter CSV filename: ").strip()
            try:
                serial_numbers = []
                with open(filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row:
                            serial_numbers.append(row[0].strip())
                
                if serial_numbers:
                    exporter.export_all_station_timestamps(serial_numbers)
                else:
                    print("No serial numbers found in file.")
            except FileNotFoundError:
                print(f"File {filename} not found.")
        
        else:  # Default to numbers.csv
            try:
                serial_numbers = []
                with open('numbers.csv', 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row:
                            serial_numbers.append(row[0].strip())
                
                if serial_numbers:
                    exporter.export_all_station_timestamps(serial_numbers)
                else:
                    print("No serial numbers found in numbers.csv.")
            except FileNotFoundError:
                print("numbers.csv file not found.")

if __name__ == "__main__":
    main()
