from typing import List, Any, Dict, Optional


# [{'ppid': 'MX0XF2C1FC600581038QA01', 'line_name': 'J01', 'GROUP_A': '2025-08-22 02:59:36', 'GROUP_B': None]
def wip_to_hour_summary(group_name: str, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Convert WIP data to hour summary with unit counts and formatted unit details.

    Args:
        group_name: The group name to analyze (GROUP_A or GROUP_B)
        data: List of WIP records with ppid, line_name, GROUP_A, GROUP_B

    Returns:
        Dictionary with summary count and formatted units, or None if no data
    """

    if data is None:
        return {
            "summary": 0,
            "units": []
        }
    if len(data) == 0:
        return {
            "summary": 0,
            "units": []
        }

    # Filter units that have a timestamp for the specified group
    units_with_group = []

    for record in data:
        ppid = record.get('ppid', '')
        line_name = record.get('line_name', '')

        units_with_group.append({
            "ppid": ppid,
            "group_name": group_name,
            "timestamp": record.get('GROUP_A'),
            "line_name": line_name
        })

    # Count total units
    total_units = len(units_with_group)

    return {
        "summary": total_units,
        "units": units_with_group
    }
