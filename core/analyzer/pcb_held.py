from typing import List, Dict, Any
from datetime import datetime, timedelta
from collections import defaultdict

def analyze_production_hiding_patterns(raw_data, line_name: str, threshold_minutes: int = 3) -> Dict[str, Any]:
    """
    Analyze production data to detect when PCBs are being held/hidden before packing.

    Args:
        line_name: Line name to analyze (e.g., 'J01', 'J02')
        threshold_minutes: Threshold in minutes to consider as "hiding" (default: 60 minutes)

    Returns:
        Dictionary with analysis results including suspicious patterns
        :param threshold_minutes:
        :param line_name:
        :param raw_data:
    """

    # Get the raw data

    if not raw_data:
        return {
            "line_name": line_name,
            "total_pcbs": 0,
            "analysis": "No data found",
            "suspicious_pcbs": [],
            "normal_pcbs": [],
            "statistics": {}
        }

    threshold_seconds = threshold_minutes * 60
    suspicious_pcbs = []
    normal_pcbs = []

    # Analyze each PCB
    for record in raw_data:
        ppid = record['ppid']
        final_inspect_ts = record['final_inspect_ts']
        packing_ts = record['packing_ts']
        diff_seconds = record['diff_seconds']

        pcb_analysis = {
            'ppid': ppid,
            'final_inspect_time': final_inspect_ts,
            'packing_time': packing_ts,
            'delay_seconds': diff_seconds,
            'delay_minutes': round(diff_seconds / 60, 2),
            'delay_hours': round(diff_seconds / 3600, 2)
        }

        if diff_seconds > threshold_seconds:
            # This PCB was held for longer than the threshold
            pcb_analysis['status'] = 'SUSPICIOUS'
            pcb_analysis['severity'] = get_severity_level(diff_seconds)
            suspicious_pcbs.append(pcb_analysis)
        else:
            pcb_analysis['status'] = 'NORMAL'
            normal_pcbs.append(pcb_analysis)

    # Calculate statistics
    all_delays = [record['diff_seconds'] for record in raw_data]
    statistics = {
        'total_pcbs': len(raw_data),
        'suspicious_count': len(suspicious_pcbs),
        'normal_count': len(normal_pcbs),
        'suspicious_percentage': round((len(suspicious_pcbs) / len(raw_data)) * 100, 2) if raw_data else 0,
        'avg_delay_minutes': round(sum(all_delays) / len(all_delays) / 60, 2) if all_delays else 0,
        'max_delay_hours': round(max(all_delays) / 3600, 2) if all_delays else 0,
        'min_delay_seconds': min(all_delays) if all_delays else 0,
        'threshold_minutes': threshold_minutes
    }

    # Detect patterns
    patterns = detect_hiding_patterns(suspicious_pcbs)

    return {
        'line_name': line_name,
        'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'statistics': statistics,
        'suspicious_pcbs': suspicious_pcbs,
        'normal_pcbs': normal_pcbs,
        'detected_patterns': patterns,
        'recommendations': generate_recommendations(statistics, patterns)
    }

def get_severity_level(delay_seconds: int) -> str:
    """
    Determine the severity level based on delay time.

    Args:
        delay_seconds: Delay in seconds

    Returns:
        Severity level string
    """
    delay_hours = delay_seconds / 3600

    if delay_hours >= 4:
        return "CRITICAL"  # More than 4 hours
    elif delay_hours >= 2:
        return "HIGH"      # 2-4 hours
    elif delay_hours >= 1:
        return "MEDIUM"    # 1-2 hours
    else:
        return "LOW"       # Less than 1 hour

def detect_hiding_patterns(suspicious_pcbs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect patterns in suspicious PCB delays, including batch hiding patterns.

    Args:
        suspicious_pcbs: List of suspicious PCB records

    Returns:
        Dictionary with detected patterns including batch hiding analysis
    """
    if not suspicious_pcbs:
        return {"pattern_detected": False, "details": "No suspicious PCBs found"}

    # Group by severity
    severity_groups = {}
    for pcb in suspicious_pcbs:
        severity = pcb['severity']
        if severity not in severity_groups:
            severity_groups[severity] = []
        severity_groups[severity].append(pcb)

    # Analyze time patterns
    packing_times = []
    for pcb in suspicious_pcbs:
        packing_time = datetime.strptime(pcb['packing_time'], '%Y-%m-%d %H:%M:%S')
        packing_times.append(packing_time.hour)

    # Find most common packing hours for suspicious PCBs
    hour_frequency = {}
    for hour in packing_times:
        hour_frequency[hour] = hour_frequency.get(hour, 0) + 1

    most_common_hours = sorted(hour_frequency.items(), key=lambda x: x[1], reverse=True)[:3]

    # NEW: Detect batch hiding patterns
    batch_patterns = detect_batch_hiding_patterns(suspicious_pcbs)

    return {
        "pattern_detected": len(suspicious_pcbs) > 0,
        "severity_breakdown": {severity: len(pcbs) for severity, pcbs in severity_groups.items()},
        "most_common_packing_hours": most_common_hours,
        "total_suspicious": len(suspicious_pcbs),
        "longest_delay_hours": max([pcb['delay_hours'] for pcb in suspicious_pcbs]) if suspicious_pcbs else 0,
        "batch_hiding_patterns": batch_patterns  # NEW METRIC
    }

# def detect_batch_hiding_patterns(suspicious_pcbs: List[Dict[str, Any]],
#                                 time_window_minutes: int = 10,
#                                 min_batch_size: int = 3) -> Dict[str, Any]:
#     """
#     Detect when PCBs are held together and then packed as a batch.
#
#     Args:
#         suspicious_pcbs: List of suspicious PCB records
#         time_window_minutes: Time window to consider PCBs as a "batch" (default: 30 minutes)
#         min_batch_size: Minimum number of PCBs to consider as a batch (default: 3)
#
#     Returns:
#         Dictionary with batch hiding pattern analysis
#     """
#     if len(suspicious_pcbs) < min_batch_size:
#         return {
#             "batch_detected": False,
#             "reason": f"Not enough suspicious PCBs (minimum {min_batch_size} required)",
#             "batches": []
#         }
#
#     # Sort PCBs by packing time
#     sorted_pcbs = sorted(suspicious_pcbs, key=lambda x: datetime.strptime(x['packing_time'], '%Y-%m-%d %H:%M:%S'))
#
#     batches = []
#     current_batch = []
#     time_window = timedelta(minutes=time_window_minutes)
#
#     for pcb in sorted_pcbs:
#         packing_time = datetime.strptime(pcb['packing_time'], '%Y-%m-%d %H:%M:%S')
#
#         if not current_batch:
#             # Start new batch
#             current_batch = [pcb]
#         else:
#             # Check if this PCB is within the time window of the current batch
#             last_packing_time = datetime.strptime(current_batch[-1]['packing_time'], '%Y-%m-%d %H:%M:%S')
#
#             if packing_time - last_packing_time <= time_window:
#                 # Add to current batch
#                 current_batch.append(pcb)
#             else:
#                 # Close current batch if it meets minimum size
#                 if len(current_batch) >= min_batch_size:
#                     batches.append(analyze_batch(current_batch))
#
#                 # Start new batch
#                 current_batch = [pcb]
#
#     # Don't forget the last batch
#     if len(current_batch) >= min_batch_size:
#         batches.append(analyze_batch(current_batch))
#
#     # Calculate batch statistics
#     batch_stats = calculate_batch_statistics(batches, suspicious_pcbs)
#
#     return {
#         "batch_detected": len(batches) > 0,
#         "total_batches": len(batches),
#         "batches": batches,
#         "batch_statistics": batch_stats,
#         "analysis_parameters": {
#             "time_window_minutes": time_window_minutes,
#             "min_batch_size": min_batch_size
#         }
#     }



def detect_batch_hiding_patterns(suspicious_pcbs: List[Dict[str, Any]],
                                 time_window_minutes: int = 5,
                                 min_batch_size: int = 3) -> Dict[str, Any]:
    """
    Enhanced batch detection using multiple clustering approaches.

    Args:
        suspicious_pcbs: List of suspicious PCB records
        time_window_minutes: Time window for packing clustering (default: 5 minutes)
        min_batch_size: Minimum number of PCBs to consider as a batch (default: 3)

    Returns:
        Dictionary with comprehensive batch hiding pattern analysis
    """
    if len(suspicious_pcbs) < min_batch_size:
        return {
            "batch_detected": False,
            "reason": f"Not enough suspicious PCBs (minimum {min_batch_size} required)",
            "batches": []
        }

    # Method 1: Packing time clustering
    packing_clusters = detect_packing_time_clusters(suspicious_pcbs, time_window_minutes, min_batch_size)

    # Method 2: Inspection time clustering
    inspection_clusters = detect_inspection_time_clusters(suspicious_pcbs, min_batch_size)

    # Method 3: Combined pattern analysis
    combined_patterns = analyze_combined_patterns(suspicious_pcbs, min_batch_size)

    # Merge and deduplicate batches
    all_batches = merge_batch_detections(packing_clusters, inspection_clusters, combined_patterns)

    # Enhanced batch analysis
    enhanced_batches = [analyze_enhanced_batch(batch) for batch in all_batches]

    # Calculate comprehensive statistics
    batch_stats = calculate_enhanced_batch_statistics(enhanced_batches, suspicious_pcbs)

    return {
        "batch_detected": len(enhanced_batches) > 0,
        "total_batches": len(enhanced_batches),
        "detection_methods": {
            "packing_clusters": len(packing_clusters),
            "inspection_clusters": len(inspection_clusters),
            "combined_patterns": len(combined_patterns)
        },
        "batches": enhanced_batches,
        "batch_statistics": batch_stats,
        "hiding_patterns": detect_hiding_patterns_advanced(enhanced_batches),
        "analysis_parameters": {
            "time_window_minutes": time_window_minutes,
            "min_batch_size": min_batch_size
        }
    }

def detect_packing_time_clusters(suspicious_pcbs: List[Dict[str, Any]],
                                 time_window_minutes: int,
                                 min_batch_size: int) -> List[List[Dict[str, Any]]]:
    """
    Detect clusters based on packing time proximity.
    """
    sorted_pcbs = sorted(suspicious_pcbs, key=lambda x: datetime.strptime(x['packing_time'], '%Y-%m-%d %H:%M:%S'))

    clusters = []
    current_cluster = []
    time_window = timedelta(minutes=time_window_minutes)

    for pcb in sorted_pcbs:
        packing_time = datetime.strptime(pcb['packing_time'], '%Y-%m-%d %H:%M:%S')

        if not current_cluster:
            current_cluster = [pcb]
        else:
            last_packing_time = datetime.strptime(current_cluster[-1]['packing_time'], '%Y-%m-%d %H:%M:%S')

            if packing_time - last_packing_time <= time_window:
                current_cluster.append(pcb)
            else:
                if len(current_cluster) >= min_batch_size:
                    clusters.append(current_cluster)
                current_cluster = [pcb]

    # Don't forget the last cluster
    if len(current_cluster) >= min_batch_size:
        clusters.append(current_cluster)

    return clusters

def detect_inspection_time_clusters(suspicious_pcbs: List[Dict[str, Any]],
                                    min_batch_size: int) -> List[List[Dict[str, Any]]]:
    """
    Detect clusters based on inspection time periods.
    """
    # Group by inspection hour first
    inspection_hour_groups = defaultdict(list)

    for pcb in suspicious_pcbs:
        inspection_time = datetime.strptime(pcb['final_inspect_time'], '%Y-%m-%d %H:%M:%S')
        hour_key = inspection_time.strftime('%Y-%m-%d %H')
        inspection_hour_groups[hour_key].append(pcb)

    clusters = []
    for hour_key, pcbs in inspection_hour_groups.items():
        if len(pcbs) >= min_batch_size:
            # Further cluster by 30-minute windows within the hour
            pcbs_sorted = sorted(pcbs, key=lambda x: datetime.strptime(x['final_inspect_time'], '%Y-%m-%d %H:%M:%S'))

            sub_clusters = []
            current_sub_cluster = []

            for pcb in pcbs_sorted:
                inspection_time = datetime.strptime(pcb['final_inspect_time'], '%Y-%m-%d %H:%M:%S')

                if not current_sub_cluster:
                    current_sub_cluster = [pcb]
                else:
                    last_inspection = datetime.strptime(current_sub_cluster[-1]['final_inspect_time'], '%Y-%m-%d %H:%M:%S')
                    if (inspection_time - last_inspection).total_seconds() <= 1800:  # 30 minutes
                        current_sub_cluster.append(pcb)
                    else:
                        if len(current_sub_cluster) >= min_batch_size:
                            sub_clusters.append(current_sub_cluster)
                        current_sub_cluster = [pcb]

            if len(current_sub_cluster) >= min_batch_size:
                sub_clusters.append(current_sub_cluster)

            clusters.extend(sub_clusters)

    return clusters

def analyze_combined_patterns(suspicious_pcbs: List[Dict[str, Any]],
                              min_batch_size: int) -> List[List[Dict[str, Any]]]:
    """
    Analyze combined patterns looking for PCBs with similar delay characteristics.
    """
    # Group by similar delay ranges and packing proximity
    delay_groups = defaultdict(list)

    for pcb in suspicious_pcbs:
        # Create delay range buckets (30-minute intervals)
        delay_minutes = pcb['delay_minutes']
        delay_bucket = int(delay_minutes // 30) * 30  # Round down to nearest 30 minutes

        packing_time = datetime.strptime(pcb['packing_time'], '%Y-%m-%d %H:%M:%S')
        packing_hour = packing_time.strftime('%Y-%m-%d %H')

        key = f"{delay_bucket}min_{packing_hour}h"
        delay_groups[key].append(pcb)

    clusters = []
    for key, pcbs in delay_groups.items():
        if len(pcbs) >= min_batch_size:
            clusters.append(pcbs)

    return clusters

def merge_batch_detections(packing_clusters: List[List[Dict[str, Any]]],
                           inspection_clusters: List[List[Dict[str, Any]]],
                           combined_patterns: List[List[Dict[str, Any]]]) -> List[List[Dict[str, Any]]]:
    """
    Merge and deduplicate batch detections from different methods.
    """
    all_batches = []
    processed_ppids = set()

    # Process packing clusters first (highest priority)
    for cluster in packing_clusters:
        ppids = {pcb['ppid'] for pcb in cluster}
        if not ppids.intersection(processed_ppids):
            all_batches.append(cluster)
            processed_ppids.update(ppids)

    # Process inspection clusters
    for cluster in inspection_clusters:
        ppids = {pcb['ppid'] for pcb in cluster}
        overlap = ppids.intersection(processed_ppids)
        if len(overlap) < len(ppids) * 0.5:  # Less than 50% overlap
            new_cluster = [pcb for pcb in cluster if pcb['ppid'] not in processed_ppids]
            if len(new_cluster) >= 3:
                all_batches.append(new_cluster)
                processed_ppids.update({pcb['ppid'] for pcb in new_cluster})

    # Process combined patterns
    for cluster in combined_patterns:
        ppids = {pcb['ppid'] for pcb in cluster}
        overlap = ppids.intersection(processed_ppids)
        if len(overlap) < len(ppids) * 0.3:  # Less than 30% overlap
            new_cluster = [pcb for pcb in cluster if pcb['ppid'] not in processed_ppids]
            if len(new_cluster) >= 3:
                all_batches.append(new_cluster)
                processed_ppids.update({pcb['ppid'] for pcb in new_cluster})

    return all_batches

def analyze_enhanced_batch(batch_pcbs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Enhanced batch analysis with additional metrics.
    """
    # Sort by times
    sorted_by_inspection = sorted(batch_pcbs, key=lambda x: datetime.strptime(x['final_inspect_time'], '%Y-%m-%d %H:%M:%S'))
    sorted_by_packing = sorted(batch_pcbs, key=lambda x: datetime.strptime(x['packing_time'], '%Y-%m-%d %H:%M:%S'))

    first_inspection = datetime.strptime(sorted_by_inspection[0]['final_inspect_time'], '%Y-%m-%d %H:%M:%S')
    last_inspection = datetime.strptime(sorted_by_inspection[-1]['final_inspect_time'], '%Y-%m-%d %H:%M:%S')
    first_packing = datetime.strptime(sorted_by_packing[0]['packing_time'], '%Y-%m-%d %H:%M:%S')
    last_packing = datetime.strptime(sorted_by_packing[-1]['packing_time'], '%Y-%m-%d %H:%M:%S')

    # Calculate periods
    inspection_period_minutes = (last_inspection - first_inspection).total_seconds() / 60
    packing_period_minutes = (last_packing - first_packing).total_seconds() / 60
    holding_period_minutes = (first_packing - last_inspection).total_seconds() / 60
    total_span_hours = (last_packing - first_inspection).total_seconds() / 3600

    # Calculate delay statistics
    delays = [pcb['delay_minutes'] for pcb in batch_pcbs]
    delay_std = calculate_standard_deviation(delays)

    # Batch uniformity (how consistent are the delays)
    uniformity_score = 1 / (1 + delay_std) if delay_std > 0 else 1

    # Hiding evidence score (higher = more suspicious)
    hiding_score = calculate_hiding_evidence_score(batch_pcbs, inspection_period_minutes, packing_period_minutes, holding_period_minutes)

    return {
        "batch_id": f"batch_{first_inspection.strftime('%Y%m%d_%H%M')}_size{len(batch_pcbs)}",
        "pcb_count": len(batch_pcbs),
        "pcbs": [pcb['ppid'] for pcb in batch_pcbs],
        "timing_analysis": {
            "inspection_period": {
                "start_time": sorted_by_inspection[0]['final_inspect_time'],
                "end_time": sorted_by_inspection[-1]['final_inspect_time'],
                "duration_minutes": round(inspection_period_minutes, 2)
            },
            "packing_period": {
                "start_time": sorted_by_packing[0]['packing_time'],
                "end_time": sorted_by_packing[-1]['packing_time'],
                "duration_minutes": round(packing_period_minutes, 2)
            },
            "holding_period": {
                "duration_minutes": round(holding_period_minutes, 2),
                "duration_hours": round(holding_period_minutes / 60, 2)
            },
            "total_span_hours": round(total_span_hours, 2)
        },
        "delay_statistics": {
            "avg_delay_hours": round(sum([pcb['delay_hours'] for pcb in batch_pcbs]) / len(batch_pcbs), 2),
            "min_delay_hours": round(min([pcb['delay_hours'] for pcb in batch_pcbs]), 2),
            "max_delay_hours": round(max([pcb['delay_hours'] for pcb in batch_pcbs]), 2),
            "delay_std_minutes": round(delay_std, 2),
            "uniformity_score": round(uniformity_score, 3)
        },
        "batch_characteristics": {
            "hiding_evidence_score": round(hiding_score, 3),
            "batch_type": classify_batch_type(inspection_period_minutes, packing_period_minutes, holding_period_minutes),
            "severity_distribution": get_batch_severity_distribution(batch_pcbs)
        }
    }

def calculate_hiding_evidence_score(batch_pcbs: List[Dict[str, Any]],
                                    inspection_period: float,
                                    packing_period: float,
                                    holding_period: float) -> float:
    """
    Calculate a score indicating likelihood of intentional hiding (0-1, higher = more suspicious).
    """
    score = 0.0

    # Large batch size increases suspicion
    batch_size = len(batch_pcbs)
    if batch_size >= 20: score += 0.3
    elif batch_size >= 10: score += 0.2
    elif batch_size >= 5: score += 0.1

    # Short inspection period but long holding suggests batching
    if inspection_period < 60 and holding_period > 120:  # Inspected quickly but held for 2+ hours
        score += 0.3

    # Very short packing period suggests batch release
    if packing_period < 10 and batch_size > 5:  # Large batch packed very quickly
        score += 0.2

    # Consistent delays suggest coordination
    delays = [pcb['delay_hours'] for pcb in batch_pcbs]
    delay_std = calculate_standard_deviation(delays)
    if delay_std < 0.5:  # Very consistent delays
        score += 0.2

    return min(score, 1.0)  # Cap at 1.0

def classify_batch_type(inspection_period: float, packing_period: float, holding_period: float) -> str:
    """
    Classify the type of batch based on timing characteristics.
    """
    if inspection_period < 30 and packing_period < 10:
        return "RAPID_BATCH"  # Quick inspection, quick packing
    elif holding_period > 300:  # More than 5 hours
        return "LONG_HOLD_BATCH"
    elif inspection_period > 60 and packing_period > 30:
        return "GRADUAL_BATCH"
    elif packing_period < 5:
        return "BURST_RELEASE"
    else:
        return "STANDARD_BATCH"

def calculate_standard_deviation(numbers: List[float]) -> float:
    """
    Calculate standard deviation of a list of numbers.
    """
    if len(numbers) < 2:
        return 0.0

    mean = sum(numbers) / len(numbers)
    variance = sum((x - mean) ** 2 for x in numbers) / (len(numbers) - 1)
    return variance ** 0.5

def detect_hiding_patterns_advanced(batches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Advanced pattern detection across all batches.
    """
    if not batches:
        return {"patterns": []}

    patterns = []

    # High hiding evidence pattern
    high_evidence_batches = [b for b in batches if b['batch_characteristics']['hiding_evidence_score'] > 0.7]
    if high_evidence_batches:
        patterns.append({
            "pattern": "HIGH_EVIDENCE_HIDING",
            "description": f"{len(high_evidence_batches)} batches with high hiding evidence scores",
            "severity": "CRITICAL" if len(high_evidence_batches) > 2 else "HIGH"
        })

    # Systematic timing pattern
    burst_releases = [b for b in batches if b['batch_characteristics']['batch_type'] == "BURST_RELEASE"]
    if len(burst_releases) > 1:
        patterns.append({
            "pattern": "SYSTEMATIC_BURST_RELEASE",
            "description": f"{len(burst_releases)} batches released in burst patterns",
            "severity": "HIGH"
        })

    # Long hold pattern
    long_holds = [b for b in batches if b['batch_characteristics']['batch_type'] == "LONG_HOLD_BATCH"]
    if long_holds:
        patterns.append({
            "pattern": "EXTENDED_HOLDING",
            "description": f"{len(long_holds)} batches held for extended periods",
            "severity": "HIGH"
        })

    return {
        "patterns": patterns,
        "total_pattern_count": len(patterns),
        "max_severity": max([p["severity"] for p in patterns]) if patterns else "NONE"
    }

def calculate_enhanced_batch_statistics(batches: List[Dict[str, Any]],
                                        all_suspicious_pcbs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics about detected batches.
    """
    if not batches:
        return {"total_pcbs_in_batches": 0}

    total_pcbs_in_batches = sum(batch['pcb_count'] for batch in batches)
    batch_sizes = [batch['pcb_count'] for batch in batches]
    hiding_scores = [batch['batch_characteristics']['hiding_evidence_score'] for batch in batches]
    holding_hours = [batch['timing_analysis']['holding_period']['duration_hours'] for batch in batches]

    return {
        "total_pcbs_in_batches": total_pcbs_in_batches,
        "percentage_in_batches": round((total_pcbs_in_batches / len(all_suspicious_pcbs)) * 100, 2),
        "batch_size_stats": {
            "avg_batch_size": round(sum(batch_sizes) / len(batch_sizes), 2),
            "largest_batch_size": max(batch_sizes),
            "smallest_batch_size": min(batch_sizes)
        },
        "hiding_evidence_stats": {
            "avg_hiding_score": round(sum(hiding_scores) / len(hiding_scores), 3),
            "max_hiding_score": round(max(hiding_scores), 3),
            "high_evidence_batches": len([s for s in hiding_scores if s > 0.7])
        },
        "timing_stats": {
            "avg_holding_hours": round(sum(holding_hours) / len(holding_hours), 2),
            "max_holding_hours": round(max(holding_hours), 2),
            "min_holding_hours": round(min(holding_hours), 2)
        },
        "batch_type_distribution": get_batch_type_distribution(batches)
    }

def get_batch_type_distribution(batches: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get distribution of batch types.
    """
    type_counts = defaultdict(int)
    for batch in batches:
        batch_type = batch['batch_characteristics']['batch_type']
        type_counts[batch_type] += 1
    return dict(type_counts)

def get_batch_severity_distribution(batch_pcbs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get the distribution of severity levels in a batch.
    """
    severity_count = defaultdict(int)
    for pcb in batch_pcbs:
        severity_count[pcb['severity']] += 1
    return dict(severity_count)

def analyze_batch(batch_pcbs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze a single batch of PCBs.

    Args:
        batch_pcbs: List of PCBs in the batch

    Returns:
        Dictionary with batch analysis
    """
    # Sort by final inspection time to understand the holding period
    sorted_by_inspection = sorted(batch_pcbs, key=lambda x: datetime.strptime(x['final_inspect_time'], '%Y-%m-%d %H:%M:%S'))
    sorted_by_packing = sorted(batch_pcbs, key=lambda x: datetime.strptime(x['packing_time'], '%Y-%m-%d %H:%M:%S'))

    first_inspection = datetime.strptime(sorted_by_inspection[0]['final_inspect_time'], '%Y-%m-%d %H:%M:%S')
    last_inspection = datetime.strptime(sorted_by_inspection[-1]['final_inspect_time'], '%Y-%m-%d %H:%M:%S')

    first_packing = datetime.strptime(sorted_by_packing[0]['packing_time'], '%Y-%m-%d %H:%M:%S')
    last_packing = datetime.strptime(sorted_by_packing[-1]['packing_time'], '%Y-%m-%d %H:%M:%S')

    # Calculate inspection period (when PCBs were being inspected and held)
    inspection_period_minutes = (last_inspection - first_inspection).total_seconds() / 60

    # Calculate packing period (how quickly they were all packed)
    packing_period_minutes = (last_packing - first_packing).total_seconds() / 60

    # Calculate holding period (from last inspection to first packing)
    holding_period_minutes = (first_packing - last_inspection).total_seconds() / 60

    return {
        "batch_id": f"batch_{first_inspection.strftime('%Y%m%d_%H%M')}",
        "pcb_count": len(batch_pcbs),
        "pcbs": [pcb['ppid'] for pcb in batch_pcbs],
        "inspection_period": {
            "start_time": sorted_by_inspection[0]['final_inspect_time'],
            "end_time": sorted_by_inspection[-1]['final_inspect_time'],
            "duration_minutes": round(inspection_period_minutes, 2)
        },
        "packing_period": {
            "start_time": sorted_by_packing[0]['packing_time'],
            "end_time": sorted_by_packing[-1]['packing_time'],
            "duration_minutes": round(packing_period_minutes, 2)
        },
        "holding_period": {
            "duration_minutes": round(holding_period_minutes, 2),
            "duration_hours": round(holding_period_minutes / 60, 2)
        },
        "avg_individual_delay_hours": round(sum([pcb['delay_hours'] for pcb in batch_pcbs]) / len(batch_pcbs), 2),
        "severity_distribution": get_batch_severity_distribution(batch_pcbs)
    }

def get_batch_severity_distribution(batch_pcbs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get the distribution of severity levels in a batch.

    Args:
        batch_pcbs: List of PCBs in the batch

    Returns:
        Dictionary with severity counts
    """
    severity_count = defaultdict(int)
    for pcb in batch_pcbs:
        severity_count[pcb['severity']] += 1
    return dict(severity_count)

def calculate_batch_statistics(batches: List[Dict[str, Any]], all_suspicious_pcbs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate overall statistics about detected batches.

    Args:
        batches: List of detected batches
        all_suspicious_pcbs: All suspicious PCBs for comparison

    Returns:
        Dictionary with batch statistics
    """
    if not batches:
        return {
            "total_pcbs_in_batches": 0,
            "percentage_in_batches": 0,
            "avg_batch_size": 0,
            "largest_batch_size": 0,
            "avg_holding_hours": 0,
            "max_holding_hours": 0
        }

    total_pcbs_in_batches = sum(batch['pcb_count'] for batch in batches)
    batch_sizes = [batch['pcb_count'] for batch in batches]
    holding_hours = [batch['holding_period']['duration_hours'] for batch in batches]

    return {
        "total_pcbs_in_batches": total_pcbs_in_batches,
        "percentage_in_batches": round((total_pcbs_in_batches / len(all_suspicious_pcbs)) * 100, 2),
        "avg_batch_size": round(sum(batch_sizes) / len(batch_sizes), 2),
        "largest_batch_size": max(batch_sizes),
        "smallest_batch_size": min(batch_sizes),
        "avg_holding_hours": round(sum(holding_hours) / len(holding_hours), 2),
        "max_holding_hours": round(max(holding_hours), 2),
        "min_holding_hours": round(min(holding_hours), 2)
    }

def generate_recommendations(statistics: Dict[str, Any], patterns: Dict[str, Any]) -> List[str]:
    """
    Generate recommendations based on the analysis, including batch hiding patterns.

    Args:
        statistics: Statistics dictionary
        patterns: Patterns dictionary

    Returns:
        List of recommendation strings
    """
    recommendations = []

    suspicious_percentage = statistics.get('suspicious_percentage', 0)

    if suspicious_percentage > 50:
        recommendations.append("CRITICAL: More than 50% of PCBs are being held for extended periods. Investigate process bottlenecks immediately.")
    elif suspicious_percentage > 25:
        recommendations.append("WARNING: Over 25% of PCBs show extended delays. Review packing station workflow and capacity.")
    elif suspicious_percentage > 10:
        recommendations.append("CAUTION: Elevated percentage of delayed PCBs detected. Monitor trend closely.")

    if statistics.get('max_delay_hours', 0) > 6:
        recommendations.append("ALERT: Some PCBs are being held for more than 6 hours. Check for hidden inventory practices.")

    if patterns.get('pattern_detected', False):
        common_hours = patterns.get('most_common_packing_hours', [])
        if common_hours:
            hour, count = common_hours[0]
            recommendations.append(f"PATTERN: Most suspicious packing activity occurs at hour {hour}:00. Focus monitoring during this time.")

    # NEW: Batch hiding recommendations
    batch_patterns = patterns.get('batch_hiding_patterns', {})
    if batch_patterns.get('batch_detected', False):
        batch_stats = batch_patterns.get('batch_statistics', {})
        total_batches = batch_patterns.get('total_batches', 0)
        percentage_in_batches = batch_stats.get('percentage_in_batches', 0)

        if percentage_in_batches > 70:
            recommendations.append(f"BATCH HIDING CRITICAL: {percentage_in_batches}% of suspicious PCBs are in {total_batches} batches. Strong evidence of intentional batch holding.")
        elif percentage_in_batches > 50:
            recommendations.append(f"BATCH HIDING WARNING: {percentage_in_batches}% of suspicious PCBs are in {total_batches} batches. Investigate batch packing practices.")
        elif total_batches > 1:
            recommendations.append(f"BATCH PATTERN: Detected {total_batches} batches of held PCBs. Monitor for systematic batch hiding.")

        avg_holding = batch_stats.get('avg_holding_hours', 0)
        if avg_holding > 2:
            recommendations.append(f"EXTENDED HOLDING: Batches are held for an average of {avg_holding} hours before packing. Investigate reasons for extended holding.")

    if not recommendations:
        recommendations.append("NORMAL: Production flow appears normal with minimal delays between final inspection and packing.")

    return recommendations