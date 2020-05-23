import pandas as pd
import json


def save_dict_to_file(dic, path):
    with open(path, 'w')  as f:
        json.dump(dic, f)


def load_file_to_dict( path):
    with open(path) as f:
        data = json.load(f)
        return data

def save_dataframe_to_file(dataframe, path):
    dfjson = dataframe.to_json(date_format='iso')
    with open(path, 'w') as outfile:
        json.dump(dfjson, outfile)


def load_file_to_dataframe(path):
    with open(path) as json_file:
        data = json.load(json_file)
    df = pd.read_json(data)
    for key in df.keys():
        if key.lower().find('date')> -1:
            df[key] = pd.to_datetime(df[key])
        elif key.lower().find('time')> -1:
            df[key] = pd.to_datetime(df[key])
    return df

