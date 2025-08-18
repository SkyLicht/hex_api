# This is a sample Python script.
from core.db.ppid_record_db import SQLiteReadOnlyConnection


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    db = SQLiteReadOnlyConnection()

    res = db.execute_query("select * from records_table where ppid = 'MX0XF2C1FC60057U086IA01' ")
    print_hi(res)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
