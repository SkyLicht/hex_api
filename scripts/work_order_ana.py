import json

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
import os
import numpy as np

DEFAULT_STATIONS = ['SMT_INPUT1','SPI1','REFLOW_VI1','AOI_B2','SMT_INPUT2','SPI2','REFLOW_VI2','AOI_T2','PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT1', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']

def _safe_percentile(sorted_values: List[float], q: float) -> Optional[float]:
    """Calculate percentile from sorted values."""
    n = len(sorted_values)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_values[0])
    pos = (n - 1) * q
    lower = int(pos)
    upper = min(lower + 1, n - 1)
    frac = pos - lower
    return float(sorted_values[lower] * (1 - frac) + sorted_values[upper] * frac)

def _safe_percentiles(values: List[float]) -> Dict[str, Optional[float]]:
    """Calculate percentiles safely handling empty lists."""
    if not values:
        return {"p25": None, "p50": None, "p75": None, "p90": None}
    s = sorted(values)
    return {
        "p25": _safe_percentile(s, 0.25),
        "p50": _safe_percentile(s, 0.50),
        "p75": _safe_percentile(s, 0.75),
        "p90": _safe_percentile(s, 0.90),
    }



def calculate_consecutive_station_times_fast(df: pd.DataFrame, error_flag: int = 0) -> Dict[str, Any]:
    """
    Ultra-fast vectorized calculation of consecutive station times.

    Args:
        df: DataFrame with process data
        error_flag: Filter by error_flag value (0 = pass, 1 = fail). Default is 0.
    """
    # Filter by error_flag first
    df_error_filtered = df[df['error_flag'] == error_flag].copy()
    print(f"Filtered to error_flag={error_flag}: {len(df_error_filtered)} records from {len(df)} total")

    # Filter and prepare data
    df_filtered = df_error_filtered[df_error_filtered['group_name'].isin(DEFAULT_STATIONS)].copy()
    df_filtered['collected_timestamp'] = pd.to_datetime(df_filtered['collected_timestamp'])
    df_filtered = df_filtered.sort_values(['ppid', 'collected_timestamp'])

    results = {}
    consecutive_pairs = [(DEFAULT_STATIONS[i], DEFAULT_STATIONS[i+1])
                         for i in range(len(DEFAULT_STATIONS)-1)]

    for from_station, to_station in consecutive_pairs:
        print(f"Processing {from_station} -> {to_station} (error_flag={error_flag})")

        # Get data for both stations
        from_data = df_filtered[df_filtered['group_name'] == from_station][['ppid', 'collected_timestamp']].copy()
        to_data = df_filtered[df_filtered['group_name'] == to_station][['ppid', 'collected_timestamp']].copy()

        if from_data.empty or to_data.empty:
            continue

        # Get last occurrence of from_station for each PPID
        from_last = from_data.groupby('ppid')['collected_timestamp'].last().reset_index()
        from_last.columns = ['ppid', 'from_time']

        # Get first occurrence of to_station for each PPID
        to_first = to_data.groupby('ppid')['collected_timestamp'].first().reset_index()
        to_first.columns = ['ppid', 'to_time']

        # Merge and calculate differences
        merged = pd.merge(from_last, to_first, on='ppid', how='inner')

        if merged.empty:
            continue

        # Calculate time differences (only where to_time > from_time)
        merged = merged[merged['to_time'] > merged['from_time']].copy()

        if merged.empty:
            continue

        merged['time_diff'] = (merged['to_time'] - merged['from_time']).dt.total_seconds()

        # Filter positive times
        valid_times = merged[merged['time_diff'] > 0]['time_diff'].tolist()

        if valid_times:
            transition_key = f"{from_station} -> {to_station}"
            pct = _safe_percentiles(valid_times)
            results[transition_key] = {
                "count": len(valid_times),
                "p25_seconds": round(pct["p25"], 1) if pct["p25"] is not None else None,
                "p50_seconds": round(pct["p50"], 1) if pct["p50"] is not None else None,
                "p75_seconds": round(pct["p75"], 1) if pct["p75"] is not None else None,
                "p90_seconds": round(pct["p90"], 1) if pct["p90"] is not None else None,
            }
            print(f"  ✅ Found {len(valid_times)} transitions")
        else:
            print(f"  ❌ No valid transitions")

    return results

def calculate_station_to_packing_times_fast(df: pd.DataFrame, error_flag: int = 0) -> Dict[str, Any]:
    """
    Ultra-fast vectorized calculation of station-to-packing times.

    Args:
        df: DataFrame with process data
        error_flag: Filter by error_flag value (0 = pass, 1 = fail). Default is 0.
    """
    # Filter by error_flag first
    df_error_filtered = df[df['error_flag'] == error_flag].copy()
    print(f"Filtered to error_flag={error_flag}: {len(df_error_filtered)} records from {len(df)} total")

    # Check if PACKING exists in filtered data
    if 'PACKING' not in df_error_filtered['group_name'].values:
        print(f"❌ No PACKING station found for error_flag={error_flag}")
        return {}

    # Filter and prepare data
    df_filtered = df_error_filtered[df_error_filtered['group_name'].isin(DEFAULT_STATIONS)].copy()
    df_filtered['collected_timestamp'] = pd.to_datetime(df_filtered['collected_timestamp'])
    df_filtered = df_filtered.sort_values(['ppid', 'collected_timestamp'])

    # Get PACKING data (first occurrence per PPID)
    packing_data = df_filtered[df_filtered['group_name'] == 'PACKING'][['ppid', 'collected_timestamp']].copy()
    packing_first = packing_data.groupby('ppid')['collected_timestamp'].first().reset_index()
    packing_first.columns = ['ppid', 'packing_time']

    results = {}
    source_stations = [station for station in DEFAULT_STATIONS if station != 'PACKING']

    for station in source_stations:
        print(f"Processing {station} -> PACKING (error_flag={error_flag})")

        # Get station data (last occurrence per PPID)
        station_data = df_filtered[df_filtered['group_name'] == station][['ppid', 'collected_timestamp']].copy()

        if station_data.empty:
            continue

        station_last = station_data.groupby('ppid')['collected_timestamp'].last().reset_index()
        station_last.columns = ['ppid', 'station_time']

        # Merge with packing data
        merged = pd.merge(station_last, packing_first, on='ppid', how='inner')

        if merged.empty:
            continue

        # Calculate time differences (only where packing_time > station_time)
        merged = merged[merged['packing_time'] > merged['station_time']].copy()

        if merged.empty:
            continue

        merged['time_diff'] = (merged['packing_time'] - merged['station_time']).dt.total_seconds()

        # Filter positive times
        valid_times = merged[merged['time_diff'] > 0]['time_diff'].tolist()

        if valid_times:
            transition_key = f"{station} -> PACKING"
            pct = _safe_percentiles(valid_times)
            results[transition_key] = {
                "count": len(valid_times),
                "p25_seconds": round(pct["p25"], 1) if pct["p25"] is not None else None,
                "p50_seconds": round(pct["p50"], 1) if pct["p50"] is not None else None,
                "p75_seconds": round(pct["p75"], 1) if pct["p75"] is not None else None,
                "p90_seconds": round(pct["p90"], 1) if pct["p90"] is not None else None,
            }
            print(f"  ✅ Found {len(valid_times)} transitions")
        else:
            print(f"  ❌ No valid transitions")

    return results

def export_results_to_excel(consecutive_times: Dict[str, Any], to_packing_times: Dict[str, Any],
                            file_path: str, work_order: str = None, error_flag: int = 0) -> str:
    """
    Export analysis results to Excel file with multiple sheets.

    Args:
        consecutive_times: Results from consecutive station analysis
        to_packing_times: Results from station-to-packing analysis
        file_path: Output file path
        work_order: Work order number for metadata
        error_flag: Error flag filter used (0 = pass, 1 = fail)
    """
    try:
        # Prepare consecutive transitions data
        consecutive_rows = []
        for transition, stats in consecutive_times.items():
            from_station, to_station = transition.split(' -> ')
            consecutive_rows.append({
                'from_station': from_station,
                'to_station': to_station,
                'transition': transition,
                'count': stats['count'],
                'p25_seconds': stats['p25_seconds'],
                'p50_seconds': stats['p50_seconds'],
                'p75_seconds': stats['p75_seconds'],
                'p90_seconds': stats['p90_seconds'],
                'p25_minutes': round(stats['p25_seconds']/60, 2) if stats['p25_seconds'] else None,
                'p50_minutes': round(stats['p50_seconds']/60, 2) if stats['p50_seconds'] else None,
                'p75_minutes': round(stats['p75_seconds']/60, 2) if stats['p75_seconds'] else None,
                'p90_minutes': round(stats['p90_seconds']/60, 2) if stats['p90_seconds'] else None,
            })

        # Prepare station-to-packing data
        packing_rows = []
        for transition, stats in to_packing_times.items():
            from_station = transition.split(' -> ')[0]
            packing_rows.append({
                'from_station': from_station,
                'to_station': 'PACKING',
                'transition': transition,
                'count': stats['count'],
                'p25_seconds': stats['p25_seconds'],
                'p50_seconds': stats['p50_seconds'],
                'p75_seconds': stats['p75_seconds'],
                'p90_seconds': stats['p90_seconds'],
                'p25_minutes': round(stats['p25_seconds']/60, 2) if stats['p25_seconds'] else None,
                'p50_minutes': round(stats['p50_seconds']/60, 2) if stats['p50_seconds'] else None,
                'p75_minutes': round(stats['p75_seconds']/60, 2) if stats['p75_seconds'] else None,
                'p90_minutes': round(stats['p90_seconds']/60, 2) if stats['p90_seconds'] else None,
            })

        # Create DataFrames
        consecutive_df = pd.DataFrame(consecutive_rows)
        packing_df = pd.DataFrame(packing_rows)

        # Create summary metadata
        error_flag_desc = "PASS (error_flag=0)" if error_flag == 0 else "FAIL (error_flag=1)"
        metadata = {
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'work_order': work_order or 'All',
            'error_flag_filter': error_flag,
            'error_flag_description': error_flag_desc,
            'consecutive_transitions': len(consecutive_rows),
            'packing_transitions': len(packing_rows),
            'total_transitions': len(consecutive_rows) + len(packing_rows),
            'default_stations': ', '.join(DEFAULT_STATIONS)
        }
        metadata_df = pd.DataFrame([metadata])

        print(f"Preparing export for {error_flag_desc}:")
        print(f"  Consecutive transitions: {len(consecutive_rows)}")
        print(f"  Station-to-packing transitions: {len(packing_rows)}")

        # Try Excel first
        if file_path.lower().endswith(".xlsx"):
            try:
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    # Always write summary
                    metadata_df.to_excel(writer, sheet_name='Summary', index=False)
                    print("  ✅ Written Summary sheet")

                    # Write consecutive transitions if available
                    if len(consecutive_rows) > 0:
                        consecutive_df.to_excel(writer, sheet_name='Consecutive_Transitions', index=False)
                        print("  ✅ Written Consecutive_Transitions sheet")
                    else:
                        print("  ⚠️ No consecutive transitions to write")

                    # Write station-to-packing if available
                    if len(packing_rows) > 0:
                        packing_df.to_excel(writer, sheet_name='Station_to_Packing', index=False)
                        print("  ✅ Written Station_to_Packing sheet")
                    else:
                        print("  ⚠️ No station-to-packing transitions to write")

                print(f"✅ Results exported to Excel: {file_path}")
                return file_path

            except ImportError:
                print("⚠️ Excel engine not available, falling back to CSV")
                # Fallback to multiple CSV files
                base_path = os.path.splitext(file_path)[0]

                # Save summary
                summary_path = f"{base_path}_summary.csv"
                metadata_df.to_csv(summary_path, index=False)
                print(f"  ✅ Summary saved to: {summary_path}")

                # Save consecutive if available
                if len(consecutive_rows) > 0:
                    consecutive_path = f"{base_path}_consecutive.csv"
                    consecutive_df.to_csv(consecutive_path, index=False)
                    print(f"  ✅ Consecutive transitions saved to: {consecutive_path}")

                # Save packing if available
                if len(packing_rows) > 0:
                    packing_path = f"{base_path}_to_packing.csv"
                    packing_df.to_csv(packing_path, index=False)
                    print(f"  ✅ Station-to-packing saved to: {packing_path}")

                return summary_path
        else:
            # For non-Excel files, save as single CSV with both datasets
            if len(consecutive_rows) > 0 and len(packing_rows) > 0:
                # Combine both datasets
                all_rows = consecutive_rows + packing_rows
                combined_df = pd.DataFrame(all_rows)
                combined_df.to_csv(file_path, index=False)
                print(f"✅ Combined results exported to CSV: {file_path}")
            elif len(consecutive_rows) > 0:
                consecutive_df.to_csv(file_path, index=False)
                print(f"✅ Consecutive transitions exported to CSV: {file_path}")
            elif len(packing_rows) > 0:
                packing_df.to_csv(file_path, index=False)
                print(f"✅ Station-to-packing transitions exported to CSV: {file_path}")

            return file_path

    except Exception as e:
        print(f"❌ Export failed: {e}")
        raise

def open_csv_file(file_path):
    """Open and read a CSV file."""
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Opened CSV: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return None
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return None

if __name__ == '__main__':
    # Load data
    print("Loading data...")
    df = open_csv_file('C:/Users/jorgeortiza/OneDrive - Foxconn/Documents/work_order_390017874.csv')

    if df is not None:
        # Parse timestamps
        df['collected_timestamp'] = pd.to_datetime(df['collected_timestamp'])
        work_order = df['work_order'].iloc[0] if not df.empty else None

        print(f"Data: {len(df)} records, {df['ppid'].nunique()} PPIDs, Work Order: {work_order}")

        # Calculate consecutive transitions
        print("\nCalculating consecutive station transitions...")
        consecutive_times = calculate_consecutive_station_times_fast(df)
        print(f"✅ Found {len(consecutive_times)} consecutive transitions")

        # Calculate station-to-packing
        print("\nCalculating station-to-packing times...")
        to_packing_times = calculate_station_to_packing_times_fast(df)
        print(f"✅ Found {len(to_packing_times)} station-to-packing transitions")

        # Display results
        if consecutive_times:
            print("\nConsecutive Station Transitions:")
            for transition, stats in consecutive_times.items():
                print(f"  {transition}: Count={stats['count']}, p50={stats['p50_seconds']}s")

        if to_packing_times:
            print("\nStation-to-Packing Times:")
            for transition, stats in to_packing_times.items():
                print(f"  {transition}: Count={stats['count']}, p50={stats['p50_seconds']}s")

        # Export results
        if consecutive_times or to_packing_times:
            print("\nExporting to Excel...")
            input_name = os.path.splitext(os.path.basename('C:/Users/jorgeortiza/OneDrive - Foxconn/Documents/work_order_390017874.csv'))[0]
            output_path = f'C:/Users/jorgeortiza/OneDrive - Foxconn/Documents/{input_name}_analysis.xlsx'

            export_results_to_excel(consecutive_times, to_packing_times, output_path, work_order)
        else:
            print("❌ No results to export")

    print('\nDone!')




def create_repair_report(df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    Create a repair report for units with error_flag = 1.
    Groups all records by PPID.

    Returns:
        Dict with PPID as key and list of repair records as value
    """
    # Filter records with error_flag = 1
    failed_units = df[df['error_flag'] == 1].copy()

    print(f"Found {len(failed_units)} records with error_flag = 1")
    print(f"Unique failed PPIDs: {failed_units['ppid'].nunique()}")

    # Group by PPID
    repair_report = {}

    for ppid, group in failed_units.groupby('ppid'):
        # Convert each record to dictionary, excluding 'id' field
        records = []
        for _, row in group.iterrows():
            record = {
                'ppid': row['ppid'],
                'work_order': row['work_order'],
                'collected_timestamp': row['collected_timestamp'],
                'employee_name': row['employee_name'],
                'group_name': row['group_name'],
                'line_name': row['line_name'],
                'station_name': row['station_name'],
                'model_name': row['model_name'],
                'error_flag': row['error_flag'],
                'next_station': row['next_station']
            }
            records.append(record)

        repair_report[ppid] = records

    return repair_report

def print_repair_summary(repair_report: Dict[str, List[Dict[str, Any]]]):
    """Print summary statistics of the repair report."""
    print("\n" + "="*60)
    print("REPAIR REPORT SUMMARY")
    print("="*60)

    total_ppids = len(repair_report)
    total_records = sum(len(records) for records in repair_report.values())

    print(f"Total failed PPIDs: {total_ppids}")
    print(f"Total failure records: {total_records}")

    # Station breakdown
    station_counts = {}
    for records in repair_report.values():
        for record in records:
            station = record['group_name']
            station_counts[station] = station_counts.get(station, 0) + 1

    print(f"\nFailures by station:")
    for station, count in sorted(station_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {station}: {count} failures")

    # Multiple failures
    multiple_failures = [ppid for ppid, records in repair_report.items() if len(records) > 1]
    print(f"\nPPIDs with multiple failures: {len(multiple_failures)}")

    if multiple_failures:
        print("Examples of multiple failures:")
        for ppid in multiple_failures[:3]:  # Show first 3 examples
            print(f"  {ppid}: {len(repair_report[ppid])} failures")

# if __name__ == '__main__':
#     # Load CSV file
#     csv_path = 'C:/Users/jorgeortiza/OneDrive - Foxconn/Documents/work_order_390017874.csv'
#     df = open_csv_file(csv_path)
#
#     if df is not None:
#         # Create repair report
#         repair_report = create_repair_report(df)
#
#         # Print summary
#         print_repair_summary(repair_report)
#
#         # Print JSON report
#         print("\n" + "="*60)
#         print("REPAIR REPORT JSON")
#         print("="*60)
#
#         # Convert to JSON and print
#         json_report = json.dumps(repair_report, indent=2)
#         print(json_report)
#
#         # Optionally save to file
#         output_file = 'C:/Users/jorgeortiza/OneDrive - Foxconn/Documents/repair_report.json'
#         try:
#             with open(output_file, 'w') as f:
#                 f.write(json_report)
#             print(f"\n✅ Repair report saved to: {output_file}")
#         except Exception as e:
#             print(f"❌ Failed to save report: {e}")
#
#     print("\nDone!")
