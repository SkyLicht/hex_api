import asyncio
import json
import re
from collections import OrderedDict
from datetime import datetime, timedelta
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

def transform_date_to_mackenzie(date_str):
    # Parse the input string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    # Format the datetime object to the desired output string
    return date_obj.strftime('%Y%m%d')

def transform_range_of_dates(form: str, at: str)-> list[str]:
    # Parse the input string to a datetime object
    # And return a list of dates in the range
    # Return a list of dates (str '%Y-%m-%d') in the range
    form_date = datetime.strptime(form, '%Y-%m-%d')
    at_date = datetime.strptime(at, '%Y-%m-%d')
    date_list = []
    while form_date <= at_date:
        date_list.append(form_date.strftime('%Y-%m-%d'))
        form_date += timedelta(days=1)
    return date_list


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



async def get_current_day_data_from_api():
    _date = datetime.now().strftime('%Y-%m-%d')
    responds = await api_respond_to_model(
        get_all_day(transform_date_to_mackenzie(_date)),_date)

    if responds.items() is None: return None

    return json.dumps([_r.to_dict() for keys, _r in responds.items()], indent=4)



if __name__ == "__main__":
    asyncio.run(get_current_day_data_from_api())



# async def main():
#     # Define the start and end of January 2025
#     start_date = datetime(2025, 6, 1)
#     end_date = datetime(2025, 8, 7)
#
#     # Generate all the days in January 2025
#     january_days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
#
#     # Format dates as strings
#     january_days_str = [day.strftime('%Y-%m-%d') for day in january_days]
#     data = []
#     for day in january_days_str:
#         print(day)
#
#         responds = await api_respond_to_model(
#             get_all_day(transform_date_to_mackenzie(day)),day)
#         if responds.items() is None: continue
#         for keys, records in responds.items():
#             data.append(records.to_dict())
#
#
#     df = pd.DataFrame(data)
#     df.to_excel('humber_data.xlsx', index=False)
#
#     # Save to json file
#     # import json
#     # with open("../../data/data.json", "w") as f:
#     #     json.dump(data, f, indent=4)
