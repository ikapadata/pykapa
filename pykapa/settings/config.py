from pykapa.settings.logging import logger
import yaml
import pykapa
import os


BASEPATH = os.path.dirname(pykapa.__file__)


def get_test_path_for_filename(name):
    config_path = os.path.join(BASEPATH, 'tests', name)
    logger.info(config_path)
    return config_path


def get_config_path_for_filename(name):
    config_path = os.path.join(BASEPATH, 'config', name+".yaml")
    logger.info(config_path)
    return config_path


def load_config_file(name):
    try:
        with open(get_config_path_for_filename(name), 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return None
    return data


def save_config_to_file(config, name):
    with open(get_config_path_for_filename(name), 'w') as f:
        data = yaml.dump(config, f)
    return data


config_paths = load_config_file('paths')


def build_config_from_input(config_name):
    config_name = input('Please confirm config file name (this will save config to disk) or leave blank (wont save): ')
    google_sheet_url = input('Enter the link to your Google Sheet: ')
    email = input('Enter SurveyCTO server username/email: ')
    password = input('Enter SurveyCTO server password: ')
    host = input('Enter SurveyCTO server host/domain (server_name) : ')
    slack_token = input('Enter Slack Bot Token: ')
    err_chnl = input('Enter Slack error channel name: ').lower()
    config = {
        'slack': {
            'token': slack_token,
            'error_channel': err_chnl,

        },
        'google': {
            'sheet_url': google_sheet_url,
        },
        'scto': {
            'username': email,
            'password': password,
            'host': host
        }
    }
    if config_name:
        save_config_to_file(config, config_name)
        return config_name, load_config_file(config_name)
    else:
        return config


def user_inputs():
    # google sheet url
    logger.info('Enter name of your config/project: eg: test')
    config_name = input('config: ')
    config = load_config_file(config_name)
    if not config:
        config_name, config = build_config_from_input(config_name)
    return config_name, config