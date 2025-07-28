import configparser
import argparse
import uvicorn


def start_server():
    _host = None
    _port = None
    _reload = None

    parser = argparse.ArgumentParser(description='Select the server.')
    parser.add_argument(
        '--server',
        #default= 'house',
        type=str,
        choices=['house', 'work_1', 'work_2', 'fuzion', 'production', 'default'],
        help='Select the server to run (e.g., house, work)'
    )
    args = parser.parse_args()
    # Create a ConfigParser instance
    configs = configparser.ConfigParser()
    configs.read('configs/api_config.ini')

    if args.server == 'house':
        _host = configs.get('server_house', 'host')
        _port = configs.getint('server_house', 'port')
        _reload = configs.getboolean('server_house', 'reload')
    elif args.server == 'work_1':
        _host = configs.get('server_work_1', 'host')
        _port = configs.getint('server_work_1', 'port')
        _reload = configs.getboolean('server_work_1', 'reload')
    elif args.server == 'work_2':
        _host = configs.get('server_work_2', 'host')
        _port = configs.getint('server_work_2', 'port')
        _reload = configs.getboolean('server_work_2', 'reload')
    elif args.server == 'fuzion':
        _host = configs.get('server_fuzion', 'host')
        _port = configs.getint('server_fuzion', 'port')
        _reload = configs.getboolean('server_fuzion', 'reload')
    elif args.server == 'production':
        _host = configs.get('server_production', 'host')
        _port = configs.getint('server_production', 'port')
        _reload = configs.getboolean('server_production', 'reload')
    elif args.server == 'default':
        _host = '127.0.0.1'
        _port = 8000
        _reload = False
    else:
        #Set default values
        configs['server'] = {
            'host': '127.0.0.1',
            'port': '8000',
            'reload': 'False'
        }
        return



    try:

        print('Starting server...')

        uvicorn.run(
            "core.api.main:app",
            host=_host,
            port=_port,
            reload=_reload  # Remove reload=True in production or set in config.ini
        )

        print('Server started.')



    except Exception as e:
        print('Server failed to start.')
        print(e)
        pass


if __name__ == "__main__":
    start_server()
