from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict
import json

class DeltaAnalyzer:
    """
    Analyzes PPID records to calculate time deltas between consecutive records
    and groups them by minute intervals.
    """

    def __init__(self, ppid_records: List[Dict[str, Any]]):
        """
        Initialize with PPID records data.

        Args:
            ppid_records: List of PPID record dictionaries from API response
        """
        self.ppid_records = ppid_records
        self.deltas = []
        self.grouped_deltas = {}

        # Sort records by timestamp (oldest first) for proper delta calculation
        self.sorted_records = sorted(
            ppid_records,
            key=lambda x: datetime.strptime(x['collected_timestamp'], "%Y-%m-%d %H:%M:%S")
        )

    def calculate_deltas(self) -> List[Dict[str, Any]]:
        """
        Calculate time deltas between consecutive PPID records.
        
        Returns:
            List of dictionaries containing delta information
        """
        self.deltas = []
        
        for i in range(1, len(self.sorted_records)):
            current_record = self.sorted_records[i]
            previous_record = self.sorted_records[i - 1]
            
            # Parse timestamps
            current_time = datetime.strptime(current_record['collected_timestamp'], "%Y-%m-%d %H:%M:%S")
            previous_time = datetime.strptime(previous_record['collected_timestamp'], "%Y-%m-%d %H:%M:%S")
            
            # Calculate delta in seconds
            delta_seconds = (current_time - previous_time).total_seconds()
            
            delta_info = {
                'from_ppid': previous_record['ppid'],
                'to_ppid': current_record['ppid'],
                'from_timestamp': previous_record['collected_timestamp'],
                'to_timestamp': current_record['collected_timestamp'],
                'delta_seconds': int(delta_seconds),
                'delta_minutes': round(delta_seconds / 60, 2)
            }
            
            self.deltas.append(delta_info)
        
        return self.deltas

    def group_deltas_by_minutes_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group deltas by minute intervals and return simplified JSON structure.

        Returns:
            Dictionary with minute intervals as keys and simplified delta info as values
        """
        if not self.deltas:
            self.calculate_deltas()

        grouped_deltas = defaultdict(list)

        for delta in self.deltas:
            delta_minutes = delta['delta_minutes']

            # Round up to the nearest minute for grouping
            minute_group = int(delta_minutes) + (1 if delta_minutes % 1 > 0 else 0)

            # Ensure minimum 1 minute group
            if minute_group == 0:
                minute_group = 1

            group_key = f"deltas_{minute_group}_min"

            # Simplified structure for JSON response
            simplified_delta = {
                'from_timestamp': delta['from_timestamp'],
                'from_ppid': delta['from_ppid'],
                'to_timestamp': delta['to_timestamp'],
                'to_ppid': delta['to_ppid'],
                'delta_seconds': delta['delta_seconds']
            }

            grouped_deltas[group_key].append(simplified_delta)

        return dict(grouped_deltas)

    def get_analysis_json(self) -> Dict[str, Any]:
        """
        Get complete analysis in JSON format including listed deltas and hour-by-hour grouping.
        
        Returns:
            Dictionary containing statistics, all deltas (new to old), grouped deltas, and hour-by-hour data
        """
        if not self.deltas:
            self.calculate_deltas()
        
        # Get statistics
        statistics = self.get_statistics()
        
        # Get grouped deltas
        grouped_deltas = self.group_deltas_by_minutes_json()
        
        # Get all deltas in simplified format (sorted from new to old)
        listed_deltas = [
            {
                'from_timestamp': delta['from_timestamp'],
                'from_ppid': delta['from_ppid'],
                'to_timestamp': delta['to_timestamp'],
                'to_ppid': delta['to_ppid'],
                'delta_seconds': delta['delta_seconds'],
                'delta_minutes': delta['delta_minutes'],
                'hour_by_hour': self.get_hour_from_timestamp(delta['to_timestamp'])
            }
            for delta in reversed(self.deltas)  # Reverse to get new to old
        ]
        
        # Get hour-by-hour grouping
        hour_by_hour = self.group_ppids_by_hour()
        
        return {
            'statistics': statistics,
            'listed_deltas': listed_deltas,
            'grouped_deltas': grouped_deltas,
            'hour_by_hour': hour_by_hour,
            'total_groups': len(grouped_deltas)
        }

    def get_hour_from_timestamp(self, timestamp: str) -> int:
        """
        Extract hour from timestamp string.
        
        Args:
            timestamp: Timestamp in format "YYYY-MM-DD HH:MM:SS"
        
        Returns:
            Hour as integer (0-23)
        """
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        return dt.hour

    def group_ppids_by_hour(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group PPID records by hour (0-23).
        
        Returns:
            Dictionary with hours as keys and PPID records as values
        """
        hour_groups = {str(hour): [] for hour in range(24)}  # Initialize all hours 0-23
        
        for record in self.ppid_records:
            hour = self.get_hour_from_timestamp(record['collected_timestamp'])
            
            hour_groups[str(hour)].append({
                'collected_timestamp': record['collected_timestamp'],
                'ppid': record['ppid'],
                'model_name': record.get('model_name', ''),
                'station_name': record.get('station_name', '')
            })
        
        # Sort each hour's records by timestamp (newest first)
        for hour in hour_groups:
            hour_groups[hour].sort(
                key=lambda x: datetime.strptime(x['collected_timestamp'], "%Y-%m-%d %H:%M:%S"),
                reverse=True
            )
        
        return hour_groups

    def get_statistics(self) -> Dict[str, Any]:
        """
        Calculate comprehensive statistics about the deltas and hourly distribution.

        Returns:
            Dictionary containing delta statistics and hourly summary
        """
        if not self.deltas:
            return {
                'total_records': len(self.ppid_records),
                'total_deltas': 0,
                'avg_delta_seconds': 0,
                'min_delta_seconds': 0,
                'max_delta_seconds': 0,
                'avg_delta_minutes': 0,
                'hourly_summary': []
            }

        # Calculate delta statistics
        delta_seconds = [delta['delta_seconds'] for delta in self.deltas]

        # Calculate hourly summary
        hourly_summary = self.get_hourly_summary()

        return {
            'total_records': len(self.ppid_records),
            'total_deltas': len(self.deltas),
            'avg_delta_seconds': round(sum(delta_seconds) / len(delta_seconds), 2),
            'min_delta_seconds': min(delta_seconds),
            'max_delta_seconds': max(delta_seconds),
            'avg_delta_minutes': round((sum(delta_seconds) / len(delta_seconds)) / 60, 2),
            'hourly_summary': hourly_summary
        }


    def to_json(self) -> str:
        """
        Return complete analysis as JSON string.

        Returns:
            JSON string of the analysis
        """
        return json.dumps(self.get_analysis_json(), indent=2)

    def get_hourly_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary of PPID records by hour with quantities.

        Returns:
            List of dictionaries with hour and quantity
        """
        # Count records by hour
        hour_counts = {}

        for record in self.ppid_records:
            hour = self.get_hour_from_timestamp(record['collected_timestamp'])
            hour_counts[hour] = hour_counts.get(hour, 0) + 1

        # Create summary list with only hours that have records
        hourly_summary = []
        for hour in range(24):
            if hour in hour_counts:
                hourly_summary.append({
                    'hour': hour,
                    'quantity': hour_counts[hour]
                })

        # Sort by hour
        hourly_summary.sort(key=lambda x: x['hour'])

        return hourly_summary




# Usage example:
def analyze_ppid_data():
    """Example usage of the PPIDDeltaAnalyzer class."""

    # Sample data from your response
    sample_data = [
        {"id":"5d6e01a4-d5b6-40b6-b5a1-63b5da708cd4","timestamp":"2025-07-30 10:39:58","ppid":"MX0XF2C1FC60057703A4A01","employee":"4056","group_name":"PACKING","line_name":"J01","section_name":"PACKING","station_name":"PACKING","model_name":"XF2C1","error_flag":0,"created_at":"2025-07-30 16:40:02"},
        {"id":"03f5ab4c-db0e-4eab-898d-f857142f1802","timestamp":"2025-07-30 10:39:46","ppid":"MX0XF2C1FC60057703A5A01","employee":"4056","group_name":"PACKING","line_name":"J01","section_name":"PACKING","station_name":"PACKING","model_name":"XF2C1","error_flag":0,"created_at":"2025-07-30 16:40:02"},
        {"id":"eb50dcf6-d931-42a7-a394-483ce5d12ae9","timestamp":"2025-07-30 10:39:31","ppid":"MX0XF2C1FC60057703CXA01","employee":"4056","group_name":"PACKING","line_name":"J01","section_name":"PACKING","station_name":"PACKING","model_name":"XF2C1","error_flag":0,"created_at":"2025-07-30 16:40:02"},
        {"id":"9c54db6f-8340-4b92-84e8-3688cb2cdc83","timestamp":"2025-07-30 10:39:19","ppid":"MX0XF2C1FC6005770398A01","employee":"4056","group_name":"PACKING","line_name":"J01","section_name":"PACKING","station_name":"PACKING","model_name":"XF2C1","error_flag":0,"created_at":"2025-07-30 16:40:02"}
    ]

    # Create analyzer instance
    analyzer = DeltaAnalyzer(sample_data)

    # Get JSON analysis
    json_result = analyzer.get_analysis_json()

    # Print JSON
    print(json.dumps(json_result, indent=2))

    return json_result

if __name__ == "__main__":
    analyze_ppid_data()