
import re
from collections import OrderedDict
from datetime import datetime
from enum import Enum
from typing import Dict, Any

import requests
from pydantic import BaseModel



class HourByHourModel(BaseModel):
    """
    Model for the Hour by Hour data.
    date format: YYYY-MM-DD
    """

    id: str = None
    factory: str
    line: str
    date: str
    hour: int
    smt_in: int
    smt_out: int
    packing: int

    @property
    def week(self):
        return datetime.strptime(self.date, "%Y-%m-%d").isocalendar()[1]


    def to_dict(self):
        return {
            "id": self.id,
            "factory": self.factory,
            "line": self.line,
            "date": self.date,
            "hour": self.hour,
            "smt_in": self.smt_in,
            "smt_out": self.smt_out,
            "packing": self.packing
        }

    def __str__(self):
        return str(self.to_dict())


class TransType(Enum):
    SMT_IN = "INPUT",
    SMT_OUT = "OUTPUT",
    PACKING = "PACKING",


# http://10.13.89.96:83/home/reporte?entrada=2024112100&salida=202411210100&transtype=INPUT


def url(
        start_day: str,
        end_day: str,
        start_hour: str,
        end_hour: str,
        trans_type: TransType
) -> str:
    return (
        f"http://10.13.89.96:83/home/reporte?"
        f"entrada={start_day}{start_hour}"
        f"&salida={end_day}{end_hour}00"
        f"&transtype={trans_type.value[0]}"
    )


def fetch_data(
        start_day: str,
        end_day: str,
        start_hour: str,
        end_hour: str,
        trans_type: TransType
) -> Any:
    _url = url(
        start_day=start_day,
        end_day=end_day,
        start_hour=start_hour,
        end_hour=end_hour,
        trans_type=trans_type
    )
    print(f"Fetching data from URL: {_url}")
    # print(f"Fetching data from URL: {_url}")
    try:
        response = requests.get(_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data for {trans_type.name}: {e}")
        return None  # You might choose to handle this differently


def get_transactions(
        day: str,
        start_hour: str,
        end_hour: str
) -> Dict[str, Any]:
    data = {}
    transaction_types = [
        ('smt_in', TransType.SMT_IN),
        ('smt_out', TransType.SMT_OUT),
        ('packing', TransType.PACKING)
    ]

    for key, trans_type in transaction_types:
        result = fetch_data(
            start_day=day,
            end_day=day,
            start_hour=start_hour,
            end_hour=end_hour,
            trans_type=trans_type
        )
        if result is not None:
            data[key] = result
        else:
            print(f"No data returned for {key} on {day} between {start_hour} and {end_hour}")

    return data


def get_hour_by(day: str, hour: str) -> Dict[str, Any]:
    return get_transactions(day=day, start_hour=hour, end_hour=hour)


def get_all_day(day: str) -> Dict[str, Any]:
    return get_transactions(day=day, start_hour="00", end_hour="23")


async def api_respond_to_model(data, date: str):
    if not data:
        return None
    # Dictionary to store unique records with keys as "line-hour"
    unique_records = {}

    data_fields = ['smt_in', 'smt_out', 'packing']

    for field in data_fields:
        for item in data.get(field, []):
            # Extract the line identifier (e.g., 'J01')
            match = re.search(r"J\d{2}", item.get('LINE', ''))
            if not match:
                continue
            line = match.group()
            hour = item.get('HOURS', '')[:2]
            qty = item.get('QTY', 0)

            # Create a unique key for each "line-hour" combination
            key = f"{line}-{hour}"

            # Check if the record already exists
            if key in unique_records:
                # Update the existing record
                setattr(unique_records[key], field, qty)
            else:
                # Create a new record with default values
                unique_records[key] = HourByHourModel(
                    factory="A6",
                    date=date,
                    line=line,
                    hour=hour,
                    smt_in=0,
                    smt_out=0,
                    packing=0
                )
                # Set the appropriate field
                setattr(unique_records[key], field, qty)

    # Sort records by line and hour

    sorted_records = OrderedDict(sorted(unique_records.items()))

    # Print the result with formatting
    # print_records(sorted_records.values())

    return sorted_records


def print_records(records):
    """
    Utility function to print formatted records.
    """
    current_line = None
    for record in records:
        if record.line != current_line:
            if current_line is not None:
                print()  # New line between lines
            current_line = record.line
            print(f"Line: {record.line}")
            print(f"{'Date':<12} {'Hour':<6} {'SMT In':<8} {'SMT Out':<8} {'Packing':<8}")
            print("-" * 50)

        print(f"{record.date:<12} {record.hour:<6} {record.smt_in:<8} {record.smt_out:<8} {record.packing:<8}")


