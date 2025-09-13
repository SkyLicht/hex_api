# This is a sample Python script.
from core.db.ie_tool_db import IETOOLDBConnection
from core.db.sfc_clon_db import SQLiteReadOnlyConnection


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    db = IETOOLDBConnection().get_session()





    print(db)





# See PyCharm help at https://www.jetbrains.com/help/pycharm/
