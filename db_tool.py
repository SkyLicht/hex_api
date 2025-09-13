import argparse

from scripts.create_pop_work_plan import populate_work_plan, create_tables

if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Select db action')
    parser.add_argument(
        '--action',
        type=str,
        choices=['create_tables', 'populate_tables'],
        required=True,
        help='select action')


    args = parser.parse_args()

    if args.action == 'create_tables':
        create_tables()

    elif args.action == 'populate_tables':
        populate_work_plan()
