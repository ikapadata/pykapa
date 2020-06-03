from pykapa.settings.logging import logger
import yaml
import pykapa
import os


BASEPATH = os.path.dirname(pykapa.__file__)
PROJECTNAME_ENV_KEY = "PK_PROJECTNAME"


def get_test_path_for_filename(name):
    config_path = os.path.join(BASEPATH, 'tests', name)
    logger.info(config_path)
    return config_path


def get_config_path_for_filename(name):
    config_path = os.path.join(BASEPATH, 'config', name)
    logger.info(config_path)
    return config_path


def load_config_file(name):
    '''
    supports yaml only filenames excluding extension
    '''
    try:
        with open(get_config_path_for_filename(name + ".yaml"), 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return None
    for k, v in data.items():
        if not v:
            raise Exception("Missing config for field {}. Check your config file".format(k))
    return data


def save_config_to_file(config, name):
    with open(get_config_path_for_filename(name), 'w') as f:
        data = yaml.dump(config, f)
    return data


# TODO: Remove this requirement (or commit to extended csv support)
config_paths = load_config_file('paths')


def build_config_from_input():
    config_name = input('Please confirm config file name (this will save config to disk): ')
    assert config_name is not None
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
            'credentials': 'credentials.json'
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


def get_config_for_name(config_name):
    return load_config_file(config_name)


class Config:

    def __init__(self, config_name=None):
        if config_name:
            self.config = get_config_for_name(config_name)
        else:
            self.config = get_config_for_name(os.environ.get(PROJECTNAME_ENV_KEY))

        self.slack_token = self.config.get("slack").get("token")
        self.slack_error_channel = self.config.get("slack").get("error_channel")
        self.google_sheet_url = self.config.get("google").get("sheet_url")
        self.google_credentials = get_config_path_for_filename(self.config.get("google").get("credentials"))
        self.scto_username = self.config.get("scto").get("username")
        self.scto_password = self.config.get("scto").get("password")
        self.scto_host = self.config.get("scto").get("host")


def setup_config_for_project():
    # google sheet url
    config_name = input('Enter name of your config/project. eg: my_project: ')
    config = load_config_file(config_name)
    if not config:
        config_name, config = build_config_from_input()
        os.environ.setdefault(PROJECTNAME_ENV_KEY, config_name)
    return Config(config_name)
