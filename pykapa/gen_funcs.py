import os
import json
import yaml
import pandas as pd
from pykapa.config import logger
import pykapa

data_dir_path = os.getenv("DATADIR_PATH", None)
if not data_dir_path:
    logger.info("DATADIR_PATH not set. Using default './data")
    data_dir_path = "./data"

# improve config file managements
BASEPATH = os.path.dirname(pykapa.__file__)


def build_data_path(subfolder_path):
    return os.path.join(data_dir_path, subfolder_path)


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


def build_config_from_input(config_name):
    logger.info('Enter the link to your Google Sheet.')
    google_sheet_url = input('link: ')
    logger.info('\nEnter SurveyCTO server credentials.')
    email = input('Email: ')
    password = input('Password: ')
    server = input('Server: ')
    logger.info('\nEnter Slack Info')
    bot_token = input('Bot Token: ')
    err_chnl = input('Slack channel Name for errors: ').lower()
    config = {'SHEET_LINK': google_sheet_url, 'EMAIL': email,
              'PASSWORD': password, 'SERVER': server, 'ERR_CHNL': err_chnl}
    save_config_to_file(config, config_name)
    return load_config_file(config_name)


def user_inputs():
    # google sheet url
    logger.info('Enter name of your config/project: eg: test')
    config_name = input('config: ')
    config = load_config_file(config_name)
    if not config:
        config = build_config_from_input(config_name)
    return config


# read json file
def read_json_file(filepath):
    if os.path.isfile(filepath) == True:
        with open(filepath, 'r') as jsonX:
            json_file = json.load(jsonX)

        if json_file == '[]':
            json_file = ast.literal_eval(json_file)
        else:
            json_file == json_file
    return json_file


# write to json file
def write_to_json(filepath, data):
    if os.path.isfile(filepath) == True:
        with open(filepath, 'w') as file:
            json.dump(data, file)

    else:
        # obtain directory
        filename = filepath.split('/')[-1]
        dir_file = filepath.replace('/%s' % filename, '')
        os.makedirs(dir_file)
        # write to json file
        with open(filepath, 'w') as file:
            json.dump(data, file)

    # read the file
    json_file = read_json_file(filepath)

    return json_file


# directory relative to the script
def make_relative_dir(*folders_and_filename):
    script_path = os.path.abspath('__file__')  # path to current script
    script_dir = os.path.split(script_path)[0]  # i.e. /path/to/dir/
    abs_file_path = os.path.join(script_dir, *folders_and_filename)
    return abs_file_path


# remove special characters from a string
def remove_specialchar(string):
    return ''.join(e for e in string if e.isalnum())


# substring after the last given char
def substr_after_char(string, char):
    idx = string.rfind(str(char))
    if idx >= 0:
        return string[idx + 1:len(string)]
    else:
        return None


# create a json database 
def create_json_file(filepath):
    filename = os.path.basename(filepath)
    Dir = os.path.dirname(filepath)
    filelist = os.listdir(Dir)

    # create empty database table if it doesn't exist
    if filename not in filelist:
        file = open(filepath, "w")
        json.dump('[]', file)
        file.close()


# writing new data to csv file
def local_csv(filepath, df_survey):
    if os.path.isfile(filepath):
        logger.info('APPENDING NEW CSV:')
        df_0 = pd.read_csv(filepath)
        # append unique data
        df_ = pd.concat([df_0, df_survey]).drop_duplicates(subset=['KEY'], keep='first')[list(df_survey)]
        df_str = df_.astype(str).replace('nan', '', regex=True)
        df_str.to_csv(filepath, index=False)
    else:
        logger.info('WRITING NEW CSV:')
        df_str = df_survey.astype(str).replace('nan', '', regex=True)
        df_str.to_csv(filepath, index=False)
