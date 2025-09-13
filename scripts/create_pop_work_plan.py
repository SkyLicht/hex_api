import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

import json
from core.data.dao.planner.facorty_dao import FactoryDAO
from core.data.dao.planner.line_dao import LineDAO
from core.data.dao.planner.platform_dao import PlatformDAO
from core.data.orm_models.work_plan_model_v1 import FactoryModel, LineModel, PlatformModel, WorkPlanModel, UPHRecordORM
from core.db.ie_tool_db import IETOOLDBConnection



def create_tables():
    IETOOLDBConnection().create_table(FactoryModel)
    IETOOLDBConnection().create_table(LineModel)
    IETOOLDBConnection().create_table(PlatformModel)
    IETOOLDBConnection().create_table(WorkPlanModel)
    IETOOLDBConnection().create_table(UPHRecordORM)



def create_factories_form_json():
    try:
        # Open permissions.json file
        with open('configs/factories_dict.json') as f:
            factories = json.load(f)

        # Create default permissions

        dao = FactoryDAO(IETOOLDBConnection().get_session())

        dao.create_all([FactoryModel(id=factory['id'], name=factory["name"]) for factory in factories])

    except FileNotFoundError:
        print("factories.json file not found")
        return


def create_lines_from_json():
    try:
        # Open permissions.json file
        with open('configs/lines_dict.json') as f:
            lines = json.load(f)

        # Create default permissions

        dao = LineDAO(IETOOLDBConnection().get_session())
        dao.create_one_by_one([LineModel(id=line['id'], name=line["name"], factory_id=line['factory_id']) for line in lines])

    except FileNotFoundError:
        print("lines.json file not found")
        return


def create_platforms_from_json():
    try:
        # Open permissions.json file
        with open('configs/platforms_dict.json') as f:
            platforms = json.load(f)

        # Create default permissions

        dao = PlatformDAO(IETOOLDBConnection().get_session())
        dao.create_all([PlatformModel(
            id=platform['id'],
            f_n=platform['f_n'],
            platform=platform["platform"],
            sku=platform['sku'],
            uph=platform['uph'],
            cost=platform['cost'],
            components=platform['components'],
            components_list_id=platform['components_list_id'],
            width=platform['width'],
            height=platform['height'],

        ) for platform in platforms])

    except FileNotFoundError:
        print("platforms.json file not found")
        return

def populate_work_plan():
    create_factories_form_json()
    create_lines_from_json()
    create_platforms_from_json()


#
# if __name__ == "__main__":
#     create_pop_work_plan_tables()
