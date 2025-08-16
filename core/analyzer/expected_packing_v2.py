from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import statistics

def _safe_percentile(sorted_values: List[float], q: float) -> Optional[float]:
    """
    Compute percentile with linear interpolation, robust for very small samples.

    Args:
        sorted_values: values sorted ascending
        q: quantile in [0,1], e.g., 0.25 for p25

    Returns:
        percentile value or None if list empty
    """
    n = len(sorted_values)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_values[0])
    # Linear interpolation between closest ranks
    pos = (n - 1) * q
    lower = int(pos)
    upper = min(lower + 1, n - 1)
    frac = pos - lower
    return float(sorted_values[lower] * (1 - frac) + sorted_values[upper] * frac)

def _safe_percentiles(values: List[float]) -> Dict[str, Optional[float]]:
    """
    Return p25, p75, p90 with robust handling of small samples.
    """
    if not values:
        return {"p25": None, "p75": None, "p90": None}
    s = sorted(values)
    return {
        "p25": _safe_percentile(s, 0.25),
        "p75": _safe_percentile(s, 0.75),
        "p90": _safe_percentile(s, 0.90),
    }

def calculate_cycle_time_analysis(process_flow_data: List[Dict[str, Any]],
                                  target_hour_start: str,
                                  target_hour_end: str) -> Dict[str, Any]:
    """
    Cycle time-based analysis that calculates expected arrival times at each station
    based on historical cycle times between stations.

    Example: If FINAL_VI completes at 10:05:20 and cycle time FINAL_VI→FINAL_INSPECT is 45s,
    then expected FINAL_INSPECT time is 10:06:05.
    """

    # Parse target time window
    target_start = datetime.strptime(target_hour_start, '%Y-%m-%d %H:%M:%S')
    target_end = datetime.strptime(target_hour_end, '%Y-%m-%d %H:%M:%S')

    # Step 1: Calculate historical cycle times between each station pair
    cycle_times = calculate_historical_cycle_times(process_flow_data)

    # Step 2: For each unit that completed a station in target hour, calculate expected downstream times
    expected_arrivals = calculate_expected_station_arrivals(
        process_flow_data, target_start, target_end, cycle_times
    )

    # Step 3: Compare expected vs actual arrivals to find delays and held units
    delay_analysis = analyze_expected_vs_actual_arrivals(expected_arrivals, target_start, target_end)

    # Step 4: Generate comprehensive analysis
    return {
        'target_period': {
            'start': target_hour_start,
            'end': target_hour_end,
            'duration_minutes': (target_end - target_start).total_seconds() / 60
        },
        'cycle_time_analysis': cycle_times,
        'expected_arrivals': expected_arrivals,
        'delay_analysis': delay_analysis,
        'held_units_by_station': identify_held_units_by_station(expected_arrivals),
        'recommendations': generate_cycle_time_recommendations(delay_analysis, cycle_times)
    }

def calculate_historical_cycle_times(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Calculate average, median, and percentile cycle times between each station pair.
    """
    stations = ['PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']
    station_pairs = [(stations[i], stations[i+1]) for i in range(len(stations)-1)]

    cycle_times: Dict[str, Dict[str, Any]] = {}

    for from_station, to_station in station_pairs:
        pair_key = f"{from_station}_to_{to_station}"
        cycle_data: List[float] = []

        # Collect all cycle times for this station pair
        for record in data:
            from_time = record.get(from_station)
            to_time = record.get(to_station)

            if (from_time and to_time and
                    from_time != 'nan' and to_time != 'nan'):
                try:
                    from_dt = datetime.strptime(from_time, '%Y-%m-%d %H:%M:%S')
                    to_dt = datetime.strptime(to_time, '%Y-%m-%d %H:%M:%S')

                    cycle_seconds = (to_dt - from_dt).total_seconds()

                    # Only include reasonable cycle times (exclude negative and extremely long ones)
                    if 0 < cycle_seconds < 3600:  # Between 0 and 1 hour
                        cycle_data.append(cycle_seconds)
                except (ValueError, TypeError):
                    continue

        if cycle_data:
            pct = _safe_percentiles(cycle_data)
            avg = statistics.mean(cycle_data)
            med = statistics.median(cycle_data)
            std = statistics.stdev(cycle_data) if len(cycle_data) > 1 else 0.0

            cycle_times[pair_key] = {
                'from_station': from_station,
                'to_station': to_station,
                'sample_size': len(cycle_data),
                'average_seconds': round(avg, 1),
                'median_seconds': round(med, 1),
                'p25_seconds': round(pct['p25'], 1) if pct['p25'] is not None else None,
                'p75_seconds': round(pct['p75'], 1) if pct['p75'] is not None else None,
                'p90_seconds': round(pct['p90'], 1) if pct['p90'] is not None else None,
                'min_seconds': round(min(cycle_data), 1),
                'max_seconds': round(max(cycle_data), 1),
                'std_dev_seconds': round(std, 1)
            }
        else:
            # Default values if no data available
            cycle_times[pair_key] = {
                'from_station': from_station,
                'to_station': to_station,
                'sample_size': 0,
                'average_seconds': 60.0,  # Default 1 minute
                'median_seconds': 60.0,
                'p25_seconds': 30.0,
                'p75_seconds': 90.0,
                'p90_seconds': 120.0,
                'min_seconds': 30.0,
                'max_seconds': 180.0,
                'std_dev_seconds': 30.0
            }

    return cycle_times

def calculate_expected_station_arrivals(data: List[Dict[str, Any]],
                                        target_start: datetime,
                                        target_end: datetime,
                                        cycle_times: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    For each unit that completed a station in the target hour, calculate when it should
    arrive at all downstream stations based on cycle times.

    NOTE: 'source_station' is the immediate upstream station of each downstream hop,
    and 'source_completion_time' is that upstream station's completion time (actual if
    available, otherwise the expected time computed during the walk).
    """
    stations = ['PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']
    expected_arrivals: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for record in data:
        ppid = record['ppid']

        # Check each station to see if unit completed it in target hour
        for i, start_station in enumerate(stations[:-1]):  # Exclude final station (PACKING)
            start_time = record.get(start_station)

            if start_time and start_time != 'nan':
                try:
                    start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

                    # Unit completed this station in target hour
                    if target_start <= start_dt < target_end:
                        # Walk forward hop-by-hop; current_time represents the expected time
                        # at the current station in the walk (starts at the observed start_station time).
                        current_time = start_dt

                        for j in range(i + 1, len(stations)):
                            downstream_station = stations[j]
                            upstream_station = stations[j - 1]

                            # Expected upstream time for this hop is current_time before we add this hop's CT
                            expected_upstream_dt = current_time

                            # Prefer actual upstream completion if available
                            actual_upstream_time_str = record.get(upstream_station)
                            actual_upstream_dt = None
                            if actual_upstream_time_str and actual_upstream_time_str != 'nan':
                                try:
                                    actual_upstream_dt = datetime.strptime(actual_upstream_time_str, '%Y-%m-%d %H:%M:%S')
                                except (ValueError, TypeError):
                                    actual_upstream_dt = None

                            # Get cycle time for this hop
                            pair_key = f"{upstream_station}_to_{downstream_station}"
                            cycle_time_data = cycle_times.get(pair_key, {})

                            # Use median cycle time as expected for this hop
                            expected_cycle_seconds = cycle_time_data.get('median_seconds', 60)

                            # Expected arrival at downstream = expected upstream time + hop CT
                            # If we have actual upstream completion, anchor on that; else use expected upstream time
                            anchor_upstream_dt = actual_upstream_dt or expected_upstream_dt
                            expected_downstream_dt = anchor_upstream_dt + timedelta(seconds=expected_cycle_seconds)

                            # Advance current_time for the next hop in the walk
                            current_time = expected_downstream_dt

                            # Get actual downstream arrival if available
                            actual_downstream_time_str = record.get(downstream_station)
                            actual_downstream_dt = None
                            if actual_downstream_time_str and actual_downstream_time_str != 'nan':
                                try:
                                    actual_downstream_dt = datetime.strptime(actual_downstream_time_str, '%Y-%m-%d %H:%M:%S')
                                except (ValueError, TypeError):
                                    actual_downstream_dt = None

                            # Calculate delay if actual time is available
                            delay_seconds: Optional[float] = None
                            delay_status = 'not_completed'

                            if actual_downstream_dt:
                                delay_seconds = (actual_downstream_dt - expected_downstream_dt).total_seconds()
                                if delay_seconds > 300:  # More than 5 minutes late
                                    delay_status = 'delayed'
                                elif delay_seconds < -60:  # More than 1 minute early
                                    delay_status = 'early'
                                else:
                                    delay_status = 'on_time'

                            expected_arrivals[downstream_station].append({
                                'ppid': ppid,
                                # immediate upstream for this downstream station:
                                'source_station': upstream_station,
                                # actual upstream completion if available, else expected upstream time:
                                'source_completion_time': (actual_upstream_time_str
                                                          if actual_upstream_dt
                                                          else expected_upstream_dt.strftime('%Y-%m-%d %H:%M:%S')),
                                'source_time_is_actual': bool(actual_upstream_dt),
                                'expected_arrival_time': expected_downstream_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                'actual_arrival_time': actual_downstream_time_str if actual_downstream_dt else None,
                                'delay_seconds': delay_seconds,
                                'delay_minutes': round(delay_seconds / 60, 1) if delay_seconds is not None else None,
                                'delay_status': delay_status,
                                'expected_in_target_hour': target_start <= expected_downstream_dt < target_end
                            })

                except (ValueError, TypeError):
                    continue

    # De-duplicate per station per PPID: keep the record derived from the latest upstream completion
    deduped: Dict[str, List[Dict[str, Any]]] = {}
    for station, arrivals in expected_arrivals.items():
        best_by_ppid: Dict[str, Dict[str, Any]] = {}
        for a in arrivals:
            ppid = a['ppid']
            try:
                src_dt = datetime.strptime(a['source_completion_time'], '%Y-%m-%d %H:%M:%S')
            except Exception:
                src_dt = None
            if ppid not in best_by_ppid:
                best_by_ppid[ppid] = {**a, '_src_dt': src_dt}
            else:
                prev_dt = best_by_ppid[ppid].get('_src_dt')
                if src_dt and (prev_dt is None or src_dt > prev_dt):
                    best_by_ppid[ppid] = {**a, '_src_dt': src_dt}
        # remove helper
        cleaned = []
        for v in best_by_ppid.values():
            v.pop('_src_dt', None)
            cleaned.append(v)
        deduped[station] = cleaned

    return deduped

def analyze_expected_vs_actual_arrivals(expected_arrivals: Dict[str, List],
                                        target_start: datetime,
                                        target_end: datetime) -> Dict[str, Any]:
    """
    Analyze the differences between expected and actual arrival times to identify bottlenecks.
    """
    station_analysis: Dict[str, Any] = {}

    for station, arrivals in expected_arrivals.items():
        if not arrivals:
            continue

        # Filter to only units expected to arrive in target hour
        expected_in_hour = [a for a in arrivals if a['expected_in_target_hour']]

        if not expected_in_hour:
            continue

        # Categorize arrivals
        on_time = [a for a in expected_in_hour if a['delay_status'] == 'on_time']
        delayed = [a for a in expected_in_hour if a['delay_status'] == 'delayed']
        early = [a for a in expected_in_hour if a['delay_status'] == 'early']
        not_completed = [a for a in expected_in_hour if a['delay_status'] == 'not_completed']

        # Derive actual vs held
        actual_units = on_time + delayed + early              # actually passed the station
        held_units = not_completed                            # still not passed the station

        # Calculate delay statistics for delayed units
        delay_stats = {}
        if delayed:
            delay_times = [a['delay_minutes'] for a in delayed if a.get('delay_minutes') is not None]
            if delay_times:
                delay_stats = {
                    'avg_delay_minutes': round(statistics.mean(delay_times), 1),
                    'median_delay_minutes': round(statistics.median(delay_times), 1),
                    'max_delay_minutes': round(max(delay_times), 1),
                    'min_delay_minutes': round(min(delay_times), 1)
                }

        total_expected = len(expected_in_hour)
        on_time_count = len(on_time)
        delayed_count = len(delayed)
        early_count = len(early)
        not_completed_count = len(not_completed)
        actual_units_count = len(actual_units)
        held_units_count = len(held_units)

        station_analysis[station] = {
            'total_expected': total_expected,
            'on_time_count': on_time_count,
            'delayed_count': delayed_count,
            'early_count': early_count,
            'not_completed_count': not_completed_count,
            'actual_units_count': actual_units_count,     # NEW
            'held_units_count': held_units_count,         # NEW
            'on_time_percentage': round((on_time_count / total_expected) * 100, 1) if total_expected else 0.0,
            'delayed_percentage': round((delayed_count / total_expected) * 100, 1) if total_expected else 0.0,
            'completion_rate': round((actual_units_count / total_expected) * 100, 1) if total_expected else 0.0,
            'delay_statistics': delay_stats,
            'detailed_units': {
                'on_time': on_time[:5],                # First 5 for reference
                'delayed': delayed[:10],               # First 10 for reference
                'not_completed': not_completed[:10],   # First 10 for reference
                'actual_units': actual_units[:10],     # NEW preview
                'held_units': held_units[:10]          # NEW preview
            }
        }

    return station_analysis

def identify_held_units_by_station(expected_arrivals: Dict[str, List[Dict[str, Any]]],
                                   delayed_threshold_minutes: float = 10.0) -> Dict[str, List[Dict[str, Any]]]:
    """
    Identify units that are being held (significantly delayed or not completed) at each station,
    using the full expected_arrivals lists (not truncated previews).
    """
    held_units: Dict[str, List[Dict[str, Any]]] = {}

    for station, arrivals in expected_arrivals.items():
        station_held: List[Dict[str, Any]] = []

        for unit in arrivals:
            status = unit.get('delay_status')
            delay_min = unit.get('delay_minutes')
            if status == 'not_completed':
                station_held.append({
                    'ppid': unit['ppid'],
                    'issue_type': 'not_completed',
                    'expected_time': unit['expected_arrival_time'],
                    'actual_time': unit.get('actual_arrival_time'),
                    'delay_minutes': None,
                    'source_station': unit.get('source_station')
                })
            elif status == 'delayed' and (delay_min is not None) and (delay_min >= delayed_threshold_minutes):
                station_held.append({
                    'ppid': unit['ppid'],
                    'issue_type': 'delayed',
                    'expected_time': unit['expected_arrival_time'],
                    'actual_time': unit.get('actual_arrival_time'),
                    'delay_minutes': delay_min,
                    'source_station': unit.get('source_station')
                })

        if station_held:
            held_units[station] = station_held

    return held_units

def generate_cycle_time_recommendations(delay_analysis: Dict[str, Any],
                                        cycle_times: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Generate recommendations based on cycle time analysis.
    """
    recommendations: List[str] = []

    # Identify stations with high delay rates
    problematic_stations = []
    for station, analysis in delay_analysis.items():
        delayed_percentage = analysis['delayed_percentage']
        completion_rate = analysis['completion_rate']

        if completion_rate < 80:
            problematic_stations.append((station, 'low_completion', completion_rate))
        elif delayed_percentage > 30:
            problematic_stations.append((station, 'high_delays', delayed_percentage))

    # Generate station-specific recommendations
    for station, issue_type, metric in problematic_stations:
        if issue_type == 'low_completion':
            recommendations.append(f"COMPLETION ISSUE: Only {metric:.1f}% of expected units completed {station}. Investigate capacity or process issues.")
        elif issue_type == 'high_delays':
            recommendations.append(f"DELAY ISSUE: {metric:.1f}% of units arriving late at {station}. Review upstream processes and cycle times.")

    # Identify cycle time outliers
    variable_cycle_times = []
    for pair_key, data in cycle_times.items():
        if data['sample_size'] > 5 and data['average_seconds'] > 0:  # Only consider pairs with sufficient data
            cv = data['std_dev_seconds'] / data['average_seconds']  # Coefficient of variation
            if cv > 0.5:  # High variability
                variable_cycle_times.append((pair_key, cv, data['average_seconds']))

    for pair_key, cv, avg_time in variable_cycle_times[:3]:  # Top 3 most variable
        recommendations.append(f"CYCLE TIME VARIABILITY: {pair_key} has high variability ({cv:.1%}). Standardize process to improve predictability.")

    # Overall flow recommendations (use not-completed + delayed)
    total_held_units = sum(v.get('not_completed_count', 0) + v.get('delayed_count', 0) for v in delay_analysis.values())
    if total_held_units > 20:
        recommendations.append(f"FLOW DISRUPTION: {total_held_units} units experiencing delays or holds. Implement flow monitoring and quick response procedures.")

    if not recommendations:
        recommendations.append("CYCLE TIME PERFORMANCE: Production flow meeting cycle time expectations.")

    return recommendations

# ... keep existing helpers (_safe_percentile, _safe_percentiles, etc.) ...

def _station_list() -> List[str]:
    return ['PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']

def _infer_analysis_date_from_data(process_flow_data: List[Dict[str, Any]]) -> Optional[str]:
    """
    Infer the most recent date present in the dataset across all station timestamp fields.
    Returns YYYY-MM-DD or None if no timestamps found.
    """
    stations = _station_list()
    latest_dt: Optional[datetime] = None
    for rec in process_flow_data:
        for st in stations:
            ts = rec.get(st)
            if ts and ts != 'nan':
                try:
                    dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    if (latest_dt is None) or (dt > latest_dt):
                        latest_dt = dt
                except Exception:
                    continue
    return latest_dt.strftime('%Y-%m-%d') if latest_dt else None

def analyze_hourly_cycle_times(process_flow_data: List[Dict[str, Any]],
                               analysis_date: str) -> Dict[str, Any]:
    """
    Analyze cycle time performance for each hour of a specific day.
    """
    hourly_analysis: Dict[str, Any] = {}

    for hour in range(24):
        hour_start = f"{analysis_date} {hour:02d}:00:00"
        hour_end = f"{analysis_date} {hour+1:02d}:00:00" if hour < 23 else f"{analysis_date} 23:59:59"

        analysis = calculate_cycle_time_analysis(process_flow_data, hour_start, hour_end)

        # Only include hours with meaningful activity
        if analysis['expected_arrivals'] or any(
                data['total_expected'] > 0 for data in analysis['delay_analysis'].values()
        ):
            # Summary metrics for the hour
            total_expected = sum(data['total_expected'] for data in analysis['delay_analysis'].values())
            total_delayed = sum(data['delayed_count'] for data in analysis['delay_analysis'].values())
            total_not_completed = sum(data['not_completed_count'] for data in analysis['delay_analysis'].values())
            total_actual_units = sum(  # NEW
                (data.get('on_time_count', 0) + data.get('delayed_count', 0) + data.get('early_count', 0))
                for data in analysis['delay_analysis'].values()
            )
            total_held_units = sum(     # NEW
                data.get('held_units_count', 0)
                for data in analysis['delay_analysis'].values()
            )

            hourly_analysis[f"{hour:02d}:00"] = {
                'hour': hour,
                'total_expected_arrivals': total_expected,
                'delayed_units': total_delayed,
                'not_completed_units': total_not_completed,
                'actual_units_total': total_actual_units,   # NEW
                'held_units_total': total_held_units,       # NEW
                'overall_delay_rate': round((total_delayed / total_expected * 100) if total_expected > 0 else 0, 1),
                'completion_rate': round(((total_expected - total_not_completed) / total_expected * 100) if total_expected > 0 else 0, 1),
                'station_performance': analysis['delay_analysis'],
                'held_units': analysis['held_units_by_station']
            }

    # keep the hourly cycle-time table
    hourly_analysis['hourly_cycle_time_table'] = build_hourly_cycle_time_table(process_flow_data, analysis_date)

    return hourly_analysis

def build_hourly_cycle_time_table(process_flow_data: List[Dict[str, Any]],
                                  analysis_date: str,
                                  max_cycle_seconds: int = 3600,
                                  use_historical_fallback: bool = False) -> Dict[str, Any]:
    """
    Build an hour-by-hour table of cycle-time stats between adjacent stations.

    For each hour H, includes stats for station pairs using units whose upstream
    station completion occurred in hour H. If no per-hour samples are found and
    use_historical_fallback is True, fills with overall historical stats.

    Adds debug fields: upstream_events, downstream_present, used_fallback.
    """
    stations = _station_list()
    station_pairs = [(stations[i], stations[i+1]) for i in range(len(stations)-1)]

    # Precompute overall historical stats for fallback
    historical = calculate_historical_cycle_times(process_flow_data) if use_historical_fallback else {}

    result: Dict[str, Any] = {}

    for hour in range(24):
        hour_key = f"{hour:02d}:00"
        hour_start = datetime.strptime(f"{analysis_date} {hour:02d}:00:00", '%Y-%m-%d %H:%M:%S')
        hour_end = datetime.strptime(f"{analysis_date} {hour+1:02d}:00:00", '%Y-%m-%d %H:%M:%S') if hour < 23 \
            else datetime.strptime(f"{analysis_date} 23:59:59", '%Y-%m-%d %H:%M:%S')

        pair_stats: Dict[str, Any] = {}

        for from_station, to_station in station_pairs:
            pair_key = f"{from_station}_to_{to_station}"
            cycles: List[float] = []
            upstream_events = 0
            downstream_present = 0

            for record in process_flow_data:
                from_time = record.get(from_station)
                to_time = record.get(to_station)

                if from_time and from_time != 'nan':
                    try:
                        from_dt = datetime.strptime(from_time, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        continue

                    # Restrict by upstream completion occurring in this hour
                    if hour_start <= from_dt < hour_end:
                        upstream_events += 1
                        if to_time and to_time != 'nan':
                            try:
                                to_dt = datetime.strptime(to_time, '%Y-%m-%d %H:%M:%S')
                                diff = (to_dt - from_dt).total_seconds()
                                if 0 < diff < max_cycle_seconds:
                                    cycles.append(diff)
                                    downstream_present += 1
                            except Exception:
                                continue

            used_fallback = False
            if cycles:
                pct = _safe_percentiles(cycles)
                avg = statistics.mean(cycles)
                med = statistics.median(cycles)
                std = statistics.stdev(cycles) if len(cycles) > 1 else 0.0
                pair_stats[pair_key] = {
                    'sample_size': len(cycles),
                    'average_seconds': round(avg, 1),
                    'median_seconds': round(med, 1),
                    'p25_seconds': round(pct['p25'], 1) if pct['p25'] is not None else None,
                    'p75_seconds': round(pct['p75'], 1) if pct['p75'] is not None else None,
                    'p90_seconds': round(pct['p90'], 1) if pct['p90'] is not None else None,
                    'min_seconds': round(min(cycles), 1),
                    'max_seconds': round(max(cycles), 1),
                    'std_dev_seconds': round(std, 1),
                    'upstream_events': upstream_events,
                    'downstream_present': downstream_present,
                    'used_fallback': used_fallback
                }
            else:
                # No per-hour samples; optionally fall back to historical stats
                base = {
                    'sample_size': 0,
                    'average_seconds': None,
                    'median_seconds': None,
                    'p25_seconds': None,
                    'p75_seconds': None,
                    'p90_seconds': None,
                    'min_seconds': None,
                    'max_seconds': None,
                    'std_dev_seconds': None,
                }
                if use_historical_fallback and pair_key in historical:
                    used_fallback = True
                    h = historical[pair_key]
                    base.update({
                        'sample_size': h.get('sample_size', 0),
                        'average_seconds': h.get('average_seconds'),
                        'median_seconds': h.get('median_seconds'),
                        'p25_seconds': h.get('p25_seconds'),
                        'p75_seconds': h.get('p75_seconds'),
                        'p90_seconds': h.get('p90_seconds'),
                        'min_seconds': h.get('min_seconds'),
                        'max_seconds': h.get('max_seconds'),
                        'std_dev_seconds': h.get('std_dev_seconds'),
                    })
                base.update({
                    'upstream_events': upstream_events,
                    'downstream_present': downstream_present,
                    'used_fallback': used_fallback
                })
                pair_stats[pair_key] = base

        result[hour_key] = {
            'hour': hour,
            'station_pairs': pair_stats
        }

    return result