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

class WebRawTimestampsExporter:
    def __init__(self, api_base_url: str = API_BASE_URL):
        self.api_base_url = api_base_url
        self.session = requests.Session()
    
    def parse_timestamp(self, ts_str):
        """Parse ISO timestamp for sorting"""
        if not ts_str:
            return datetime.min
        try:
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return datetime.min
    
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
    
    def process_raw_timestamps(self, serial_number: str, history_data: List[Dict]) -> Dict:
        """
        Process the raw timestamps for a serial number for the most recent cycle only.
        A cycle is determined by the most recent RECEIVE station timestamp.
        Only stations from that receiving time onwards are included.
        """
        if not history_data:
            return {
                'serial_number': serial_number,
                'vi1_end': None,
                'vi1_next_station': None,
                'vi1_next_start': None,
                'upgrade_end': None,
                'bbd_assy_station': None,
                'bbd_assy_start': None,
                'bbd_assy_end': None,
                'fla_chiflash_station': None,
                'fla_chiflash_start': None,
                'packing_end': None,
                'shipping_start': None
            }
        
        # Filter only workstation data (we need workstation_name, history_station_start_time, history_station_end_time)
        workstation_data = [
            record for record in history_data 
            if record.get('source') == 'workstation' and record.get('workstation_name')
        ]
        
        if not workstation_data:
            return {
                'serial_number': serial_number,
                'vi1_end': None,
                'vi1_next_station': None,
                'vi1_next_start': None,
                'upgrade_end': None,
                'bbd_assy_station': None,
                'bbd_assy_start': None,
                'bbd_assy_end': None,
                'fla_chiflash_station': None,
                'fla_chiflash_start': None,
                'packing_end': None,
                'shipping_start': None
            }
        
        # Find the most recent RECEIVE station start time to identify the current cycle
        receive_records = [
            record for record in workstation_data 
            if record.get('workstation_name') == 'RECEIVE'
        ]
        
        most_recent_receive_time = None
        if receive_records:
            # Sort by start time to get the most recent RECEIVE
            sorted_receives = sorted(
                receive_records, 
                key=lambda x: self.parse_timestamp(x.get('history_station_start_time'))
            )
            most_recent_receive = sorted_receives[-1]
            most_recent_receive_time = self.parse_timestamp(most_recent_receive.get('history_station_start_time'))
            
            print(f"  [{serial_number}] Most recent RECEIVE: {most_recent_receive.get('history_station_start_time')}")
            
            # Filter workstation_data to only include records from this cycle onwards
            # (records that started at or after the most recent RECEIVE start time)
            workstation_data = [
                record for record in workstation_data
                if self.parse_timestamp(record.get('history_station_start_time')) >= most_recent_receive_time
            ]
            
            print(f"  [{serial_number}] Filtered to {len(workstation_data)} station records for current cycle")
        else:
            print(f"  [{serial_number}] No RECEIVE station found - using all historical data")
        
        # Build a dictionary of station times (with lists for multiple occurrences)
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
        
        result = {
            'serial_number': serial_number,
            'vi1_end': None,
            'vi1_next_station': None,
            'vi1_next_start': None,
            'upgrade_end': None,
            'bbd_assy_station': None,
            'bbd_assy_start': None,
            'bbd_assy_end': None,
            'fla_chiflash_station': None,
            'fla_chiflash_start': None,
            'packing_end': None,
            'shipping_start': None
        }
        
        # 1. VI1 end time and next station start time
        if 'VI1' in stations:
            # Sort VI1 visits by end time to get the truly most recent one
            vi1_visits = sorted(
                stations['VI1'], 
                key=lambda x: self.parse_timestamp(x['end'])
            )
            vi1_end = vi1_visits[-1]['end']  # MOST RECENT occurrence
            result['vi1_end'] = vi1_end
            
            # Find the next station after VI1's MOST RECENT occurrence
            next_station_name = None
            next_station_start = None
            
            # Look for Disassembly or UPGRADE only
            candidates = []
            
            if 'Disassembly' in stations:
                for disassembly_time in stations['Disassembly']:
                    if disassembly_time['start'] and vi1_end and disassembly_time['start'] > vi1_end:
                        candidates.append(('Disassembly', disassembly_time['start']))
                        break
            
            if 'UPGRADE' in stations:
                for upgrade_time in stations['UPGRADE']:
                    if upgrade_time['start'] and vi1_end and upgrade_time['start'] > vi1_end:
                        candidates.append(('UPGRADE', upgrade_time['start']))
                        break
            
            # Pick whichever comes first
            if candidates:
                candidates.sort(key=lambda x: x[1])
                next_station_name = candidates[0][0]
                next_station_start = candidates[0][1]
            
            result['vi1_next_station'] = next_station_name
            result['vi1_next_start'] = next_station_start
            
            # 2. Continue following the SAME cycle
            if next_station_name == 'UPGRADE':
                # Find the specific UPGRADE that matches the start time we found
                matching_upgrade = None
                for upgrade_time in stations['UPGRADE']:
                    if upgrade_time['start'] == next_station_start:
                        matching_upgrade = upgrade_time
                        break
                
                if matching_upgrade:
                    result['upgrade_end'] = matching_upgrade['end']
                    
                    # Look for BBD OR ASSY1 after THIS specific UPGRADE
                    candidates = []
                    
                    if 'BBD' in stations:
                        for bbd_time in stations['BBD']:
                            if bbd_time['start'] and matching_upgrade['end'] and bbd_time['start'] > matching_upgrade['end']:
                                candidates.append(('BBD', bbd_time['start'], bbd_time['end']))
                                break
                    
                    if 'ASSY1' in stations:
                        for assy1_time in stations['ASSY1']:
                            if assy1_time['start'] and matching_upgrade['end'] and assy1_time['start'] > matching_upgrade['end']:
                                candidates.append(('ASSY1', assy1_time['start'], assy1_time['end']))
                                break
                    
                    if 'Assembley' in stations:
                        for assembley_time in stations['Assembley']:
                            if assembley_time['start'] and matching_upgrade['end'] and assembley_time['start'] > matching_upgrade['end']:
                                candidates.append(('Assembley', assembley_time['start'], assembley_time['end']))
                                break
                    
                    # Pick whichever comes first chronologically
                    if candidates:
                        candidates.sort(key=lambda x: x[1])  # Sort by start time
                        result['bbd_assy_station'] = candidates[0][0]
                        result['bbd_assy_start'] = candidates[0][1]
                        result['bbd_assy_end'] = candidates[0][2]
        
        # 3. BBD/ASSY1 end time and FLA/CHIFLASH start time
        if result['bbd_assy_station'] and result['bbd_assy_end']:
            prev_end = result['bbd_assy_end']
            
            # Find the earliest FLA or CHIFLASH that comes AFTER this specific BBD/ASSY1
            candidates = []
            
            if 'FLA' in stations:
                for fla_time in stations['FLA']:
                    if fla_time['start'] and prev_end and fla_time['start'] > prev_end:
                        candidates.append(('FLA', fla_time['start']))
                        break
            
            if 'CHIFLASH' in stations:
                for chiflash_time in stations['CHIFLASH']:
                    if chiflash_time['start'] and prev_end and chiflash_time['start'] > prev_end:
                        candidates.append(('CHIFLASH', chiflash_time['start']))
                        break
            
            if candidates:
                candidates.sort(key=lambda x: x[1])
                result['fla_chiflash_station'] = candidates[0][0]
                result['fla_chiflash_start'] = candidates[0][1]
        
        # 4. Packing end time and Shipping start time
        if 'PACKING' in stations:
            # Sort PACKING visits by end time to get the truly most recent one
            packing_visits = sorted(
                stations['PACKING'], 
                key=lambda x: self.parse_timestamp(x['end'])
            )
            packing_end = packing_visits[-1]['end']  # MOST RECENT occurrence
            result['packing_end'] = packing_end
            
            if 'SHIPPING' in stations:
                # Find first SHIPPING after the most recent PACKING
                for shipping_time in stations['SHIPPING']:
                    if shipping_time['start'] and packing_end and shipping_time['start'] > packing_end:
                        result['shipping_start'] = shipping_time['start']
                        break
        
        return result
    
    def export_raw_timestamps(self, serial_numbers: List[str], output_file: str = "raw_timestamps.csv", start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Export raw timestamps for multiple serial numbers
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
                result = self.process_raw_timestamps(sn, history_data)
                results.append(result)
        
        # Create CSV with raw timestamps
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Serial Number',
                'VI1 End Time',
                'Next Station After VI1',
                'Next Station Start Time',
                'Upgrade End Time',
                'BBD/ASSY1 Station',
                'BBD/ASSY1 Start Time',
                'BBD/ASSY1 End Time',
                'FLA/CHIFLASH Station',
                'FLA/CHIFLASH Start Time',
                'Packing End Time',
                'Shipping Start Time'
            ])
            
            for result in results:
                writer.writerow([
                    result['serial_number'],
                    convert_to_raw_timestamp(result['vi1_end']) if result['vi1_end'] else '',
                    result['vi1_next_station'] if result['vi1_next_station'] else '',
                    convert_to_raw_timestamp(result['vi1_next_start']) if result['vi1_next_start'] else '',
                    convert_to_raw_timestamp(result['upgrade_end']) if result['upgrade_end'] else '',
                    result['bbd_assy_station'] if result['bbd_assy_station'] else '',
                    convert_to_raw_timestamp(result['bbd_assy_start']) if result['bbd_assy_start'] else '',
                    convert_to_raw_timestamp(result['bbd_assy_end']) if result['bbd_assy_end'] else '',
                    result['fla_chiflash_station'] if result['fla_chiflash_station'] else '',
                    convert_to_raw_timestamp(result['fla_chiflash_start']) if result['fla_chiflash_start'] else '',
                    convert_to_raw_timestamp(result['packing_end']) if result['packing_end'] else '',
                    convert_to_raw_timestamp(result['shipping_start']) if result['shipping_start'] else ''
                ])
        
        print(f"\n✓ Raw timestamps exported to {output_file}")
        
        # Show a sample
        if results:
            print("\n" + "="*80)
            print("Sample data for first serial number:")
            print("="*80)
            sample = results[0]
            for key, value in sample.items():
                print(f"{key}: {value}")
        
        return results

def main():
    """
    Main function - can be used in different ways:
    1. Command line with serial numbers
    2. Read from CSV file
    3. Interactive input
    """
    import sys
    
    exporter = WebRawTimestampsExporter()
    
    if len(sys.argv) > 1:
        # Command line usage: python web_raw_timestamps.py SN123 SN456 SN789
        serial_numbers = sys.argv[1:]
        exporter.export_raw_timestamps(serial_numbers)
    else:
        # Interactive mode or CSV file
        print("Web-based Raw Timestamps Exporter")
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
                exporter.export_raw_timestamps(serial_numbers)
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
                    exporter.export_raw_timestamps(serial_numbers)
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
                    exporter.export_raw_timestamps(serial_numbers)
                else:
                    print("No serial numbers found in numbers.csv.")
            except FileNotFoundError:
                print("numbers.csv file not found.")

if __name__ == "__main__":
    main()
