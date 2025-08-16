from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import math

def calculate_expected_packing_output(process_flow_data: List[Dict[str, Any]],
                                      target_hour_start: str,  # Format: "2025-08-15 10:00:00"
                                      target_hour_end: str) -> Dict[str, Any]:
    """
    Calculate expected packing output based on station flow analysis.

    Args:
        process_flow_data: List of PCB flow records with all station timestamps
        target_hour_start: Start of target hour (e.g., "2025-08-15 10:00:00")
        target_hour_end: End of target hour (e.g., "2025-08-15 11:00:00")

    Returns:
        Dictionary with flow analysis and held unit identification
    """

    # Parse target time window
    target_start = datetime.strptime(target_hour_start, '%Y-%m-%d %H:%M:%S')
    target_end = datetime.strptime(target_hour_end, '%Y-%m-%d %H:%M:%S')

    # NEW: Use flow analysis instead of historical prediction
    flow_analysis = calculate_station_flow_analysis(process_flow_data, target_start, target_end)

    # Calculate WIP for context
    wip_by_station = defaultdict(list)
    stations = ['PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']

    for station in stations[:-1]:  # Exclude PACKING
        wip = find_wip_at_station(process_flow_data, station, target_end)
        wip_by_station[station] = wip

    return {
        'target_period': {
            'start': target_hour_start,
            'end': target_hour_end,
            'duration_minutes': (target_end - target_start).total_seconds() / 60
        },
        'station_completions': flow_analysis['station_counts'],
        'flow_analysis': flow_analysis['flow_analysis'],
        'held_units': flow_analysis['held_units'],
        'flow_summary': flow_analysis['summary'],
        'wip_analysis': dict(wip_by_station),
        'hiding_locations': identify_hiding_locations(wip_by_station),
        'recommendations': generate_flow_recommendations(flow_analysis)
    }

def calculate_station_flow_analysis(data: List[Dict[str, Any]],
                                    target_start: datetime,
                                    target_end: datetime) -> Dict[str, Any]:
    """
    Analyze station-to-station flow within the target hour to identify held units.
    """
    stations = ['PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']

    station_counts = {}

    # Count completions for each station in the target hour
    for station in stations:
        count = count_station_completions(data, station, target_start, target_end)
        station_counts[station] = count

    # Calculate flow gaps (units that should have flowed but didn't)
    flow_analysis = {}
    held_units = {}

    for i, station in enumerate(stations[:-1]):  # Exclude PACKING as it's the final station
        current_station = station
        next_station = stations[i + 1]

        current_completions = station_counts[current_station]
        next_completions = station_counts[next_station]

        # Units that completed current station but not next station = potential held units
        potential_held = current_completions - next_completions

        flow_analysis[f"{current_station}_to_{next_station}"] = {
            'current_station': current_station,
            'next_station': next_station,
            'current_station_completions': current_completions,
            'next_station_completions': next_completions,
            'held_units_count': max(0, potential_held),
            'flow_efficiency': round((next_completions / current_completions * 100) if current_completions > 0 else 0, 2)
        }

        # Identify actual held units
        if potential_held > 0:
            held_units[f"between_{current_station}_and_{next_station}"] = find_held_units_between_stations(
                data, current_station, next_station, target_start, target_end
            )

    return {
        'station_counts': station_counts,
        'flow_analysis': flow_analysis,
        'held_units': held_units,
        'summary': generate_flow_summary(flow_analysis, station_counts)
    }

def find_held_units_between_stations(data: List[Dict[str, Any]],
                                     current_station: str,
                                     next_station: str,
                                     target_start: datetime,
                                     target_end: datetime) -> List[Dict[str, Any]]:
    """
    Find units that completed current_station in the target hour but didn't complete next_station.
    """
    held_units = []

    for record in data:
        current_time = record.get(current_station)
        next_time = record.get(next_station)

        if current_time and current_time != 'nan':
            try:
                current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

                # Unit completed current station in target hour
                if target_start <= current_dt < target_end:
                    # Check if it completed next station in the same hour
                    completed_next_in_hour = False
                    next_completion_time = None

                    if next_time and next_time != 'nan':
                        try:
                            next_dt = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
                            next_completion_time = next_time
                            if target_start <= next_dt < target_end:
                                completed_next_in_hour = True
                        except (ValueError, TypeError):
                            pass

                    # If it didn't complete next station in the same hour, it's held
                    if not completed_next_in_hour:
                        held_units.append({
                            'ppid': record['ppid'],
                            'completed_station': current_station,
                            'completion_time': current_time,
                            'next_station_status': 'completed_later' if next_completion_time else 'not_completed',
                            'next_completion_time': next_completion_time,
                            'holding_location': f"between_{current_station}_and_{next_station}"
                        })
            except (ValueError, TypeError):
                continue

    return held_units

def generate_flow_summary(flow_analysis: Dict[str, Any], station_counts: Dict[str, int]) -> Dict[str, Any]:
    """
    Generate summary of flow bottlenecks and held units.
    """
    total_held = 0
    worst_bottleneck = None
    worst_efficiency = 100

    bottlenecks = []

    for flow_key, flow_data in flow_analysis.items():
        held_count = flow_data['held_units_count']
        efficiency = flow_data['flow_efficiency']

        total_held += held_count

        if held_count > 0:
            bottlenecks.append({
                'flow': flow_key,
                'current_station': flow_data['current_station'],
                'next_station': flow_data['next_station'],
                'held_units': held_count,
                'efficiency': efficiency
            })

        if efficiency < worst_efficiency and flow_data['current_station_completions'] > 0:
            worst_efficiency = efficiency
            worst_bottleneck = flow_key

    # Sort bottlenecks by number of held units
    bottlenecks.sort(key=lambda x: x['held_units'], reverse=True)

    return {
        'total_held_units': total_held,
        'worst_bottleneck': worst_bottleneck,
        'worst_efficiency': worst_efficiency,
        'bottleneck_count': len(bottlenecks),
        'top_bottlenecks': bottlenecks[:3],
        'station_completions': station_counts,
        'critical_flows': [b for b in bottlenecks if b['held_units'] > 10 or b['efficiency'] < 80]
    }

def identify_vi_to_inspect_held_units(data: List[Dict[str, Any]],
                                      target_start: datetime,
                                      target_end: datetime) -> Dict[str, Any]:
    """
    Specifically analyze FINAL_VI to FINAL_INSPECT flow to identify held units.
    """
    vi_completions = []
    inspect_completions = []
    held_units = []

    for record in data:
        vi_time = record.get('FINAL_VI')
        inspect_time = record.get('FINAL_INSPECT')

        # Count FINAL_VI completions in target hour
        if vi_time and vi_time != 'nan':
            try:
                vi_dt = datetime.strptime(vi_time, '%Y-%m-%d %H:%M:%S')
                if target_start <= vi_dt < target_end:
                    vi_completions.append({
                        'ppid': record['ppid'],
                        'vi_time': vi_time,
                        'inspect_time': inspect_time
                    })
            except (ValueError, TypeError):
                continue

        # Count FINAL_INSPECT completions in target hour
        if inspect_time and inspect_time != 'nan':
            try:
                inspect_dt = datetime.strptime(inspect_time, '%Y-%m-%d %H:%M:%S')
                if target_start <= inspect_dt < target_end:
                    inspect_completions.append({
                        'ppid': record['ppid'],
                        'inspect_time': inspect_time
                    })
            except (ValueError, TypeError):
                continue

    # Find units that completed FINAL_VI but not FINAL_INSPECT in the same hour
    for vi_unit in vi_completions:
        ppid = vi_unit['ppid']
        inspect_unit = next((unit for unit in inspect_completions if unit['ppid'] == ppid), None)

        if not inspect_unit:
            # This unit completed VI but not INSPECT in the target hour
            held_units.append({
                'ppid': ppid,
                'vi_completion': vi_unit['vi_time'],
                'inspect_status': 'completed_later' if vi_unit['inspect_time'] and vi_unit['inspect_time'] != 'nan' else 'not_completed',
                'inspect_completion': vi_unit['inspect_time'] if vi_unit['inspect_time'] and vi_unit['inspect_time'] != 'nan' else None
            })

    return {
        'vi_completions': len(vi_completions),
        'inspect_completions': len(inspect_completions),
        'held_between_vi_and_inspect': len(held_units),
        'held_units_details': held_units,
        'flow_efficiency': round((len(inspect_completions) / len(vi_completions) * 100) if vi_completions else 0, 2)
    }

def generate_flow_recommendations(flow_analysis: Dict[str, Any]) -> List[str]:
    """
    Generate recommendations based on flow analysis.
    """
    recommendations = []

    summary = flow_analysis['summary']
    total_held = summary['total_held_units']

    if total_held > 0:
        recommendations.append(f"FLOW BOTTLENECKS DETECTED: {total_held} units held between stations in this hour.")

        # Critical flow recommendations
        critical_flows = summary.get('critical_flows', [])
        if critical_flows:
            for flow in critical_flows[:2]:  # Top 2 critical flows
                recommendations.append(f"CRITICAL: {flow['held_units']} units held between {flow['current_station']} and {flow['next_station']} ({flow['efficiency']:.1f}% efficiency).")

        # Worst bottleneck
        worst_bottleneck = summary.get('worst_bottleneck')
        if worst_bottleneck:
            worst_efficiency = summary.get('worst_efficiency', 0)
            if worst_efficiency < 70:
                recommendations.append(f"IMMEDIATE ACTION REQUIRED: {worst_bottleneck} has {worst_efficiency:.1f}% efficiency.")

    # Station-specific recommendations based on flow analysis
    flow_data = flow_analysis.get('flow_analysis', {})
    for flow_key, data in flow_data.items():
        if data['held_units_count'] > 20:  # High number of held units
            recommendations.append(f"HIGH VOLUME HOLDING: {data['held_units_count']} units held at {flow_key}. Investigate capacity constraints.")
        elif data['flow_efficiency'] < 60:  # Very low efficiency
            recommendations.append(f"SEVERE BOTTLENECK: {flow_key} operating at {data['flow_efficiency']:.1f}% efficiency. Immediate intervention needed.")

    if not recommendations:
        recommendations.append("NORMAL FLOW: Station-to-station flow operating within normal parameters.")

    return recommendations

# Keep existing helper functions
def count_station_completions(data: List[Dict[str, Any]],
                              station: str,
                              start_time: datetime,
                              end_time: datetime) -> int:
    """
    Count how many units completed a station within the time window.
    """
    count = 0
    for record in data:
        station_time = record.get(station)
        if station_time and station_time != 'nan':
            try:
                completion_time = datetime.strptime(station_time, '%Y-%m-%d %H:%M:%S')
                if start_time <= completion_time < end_time:
                    count += 1
            except (ValueError, TypeError):
                continue
    return count

def find_wip_at_station(data: List[Dict[str, Any]],
                        station: str,
                        cutoff_time: datetime) -> List[Dict[str, Any]]:
    """
    Find units that completed a station but haven't been packed yet.
    """
    wip_units = []

    for record in data:
        station_time = record.get(station)
        packing_time = record.get('PACKING')

        if (station_time and station_time != 'nan' and
                (not packing_time or packing_time == 'nan')):
            try:
                station_dt = datetime.strptime(station_time, '%Y-%m-%d %H:%M:%S')
                if station_dt < cutoff_time:
                    # Calculate how long it's been waiting
                    waiting_time = (cutoff_time - station_dt).total_seconds() / 60

                    wip_units.append({
                        'ppid': record['ppid'],
                        'station_completion': station_time,
                        'waiting_time_minutes': round(waiting_time, 2),
                        'waiting_time_hours': round(waiting_time / 60, 2)
                    })
            except (ValueError, TypeError):
                continue

    return wip_units

def identify_hiding_locations(wip_by_station: Dict[str, List]) -> List[Dict[str, Any]]:
    """
    Identify where units are most likely being held.
    """
    hiding_locations = []

    for station, wip_units in wip_by_station.items():
        if wip_units:
            waiting_times = [unit['waiting_time_minutes'] for unit in wip_units]
            avg_waiting = sum(waiting_times) / len(waiting_times)
            max_waiting = max(waiting_times)

            # Determine severity based on waiting time and quantity
            severity = "LOW"
            if avg_waiting > 120:  # 2 hours average
                severity = "HIGH"
            elif avg_waiting > 60:  # 1 hour average
                severity = "MEDIUM"

            if len(wip_units) > 10:  # Many units waiting
                severity = "CRITICAL" if severity == "HIGH" else "HIGH"

            hiding_locations.append({
                'station': station,
                'wip_count': len(wip_units),
                'avg_waiting_time_minutes': round(avg_waiting, 2),
                'max_waiting_time_minutes': round(max_waiting, 2),
                'severity': severity,
                'units': wip_units[:5]  # Show first 5 units for reference
            })

    # Sort by severity and WIP count
    severity_order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    hiding_locations.sort(key=lambda x: (severity_order[x['severity']], x['wip_count']), reverse=True)

    return hiding_locations

def analyze_hourly_production_flow(process_flow_data: List[Dict[str, Any]],
                                   analysis_date: str) -> Dict[str, Any]:
    """
    Analyze production flow for each hour of a specific day using corrected flow analysis.

    Args:
        process_flow_data: List of PCB flow records
        analysis_date: Date to analyze (format: "2025-08-15")

    Returns:
        Hour-by-hour analysis of station flow and held units
    """
    hourly_analysis = {}

    # Analyze each hour of the day
    for hour in range(24):
        hour_start = f"{analysis_date} {hour:02d}:00:00"
        hour_end = f"{analysis_date} {hour+1:02d}:00:00" if hour < 23 else f"{analysis_date} 23:59:59"

        analysis = calculate_expected_packing_output(process_flow_data, hour_start, hour_end)

        # Only include hours with activity
        if (analysis['station_completions']['PACKING'] > 0 or
                analysis['flow_summary']['total_held_units'] > 0):

            hourly_analysis[f"{hour:02d}:00"] = {
                'hour': hour,
                'station_completions': analysis['station_completions'],
                'flow_gaps': analysis['flow_analysis'],
                'held_units_total': analysis['flow_summary']['total_held_units'],
                'worst_bottleneck': analysis['flow_summary']['worst_bottleneck'],
                'critical_flows': analysis['flow_summary']['critical_flows'],
                'efficiency_issues': analysis['flow_summary']['top_bottlenecks']
            }

    return hourly_analysis