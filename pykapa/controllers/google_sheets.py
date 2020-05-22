import os.path

import gspread
import numpy as np
import pytz
import slackclient
from gspread_dataframe import get_as_dataframe
from oauth2client import file, client, tools
from requests.auth import HTTPDigestAuth

from pykapa.gen_funcs import make_relative_dir
from pykapa.incentives_functions import *
from pykapa.controllers.slack import slack_post, PykapaSlackClient
from pykapa.xls_functions import *
import pandas as pd


# slack_client = PykapaSlackClient()

def lst_reduce(df_msg_xls, df_msgset_xls):
    # list of headers
    lst_msg = list(df_msg_xls)
    lst_msgset = list(df_msgset_xls)

    for element in lst_msg:
        if element in lst_msgset:
            lst_msgset.remove(element)

    return lst_msgset


# append columns
def append_col(dataframe, header):
    if type(header) == list:
        for element in header:
            dataframe[element] = ''
    elif header.replace(' ', '') == '':
        dataframe = dataframe
    else:
        dataframe[header] = ''

    return dataframe


# merge data from two worksheets according to channel_id into one dataframe
def xls_merge_ws(df_msg_xls, df_msgset_xls):
    lst_msg_id = df_msg_xls.loc[:, 'channel_id']  # list of message IDs in messages worksheet
    lst_chn_id = df_msgset_xls.loc[:, 'channel_id']  # list of message IDs in messages_settings worksheet

    lst_headers = lst_reduce(df_msg_xls, df_msgset_xls)  # list of headers to append

    # append headers
    df_msg_xls = append_col(df_msg_xls, lst_headers)
    # merge the two datasets into one dataframe
    for element in lst_headers:
        for i in range(len(lst_chn_id)):
            for j in range(len(lst_msg_id)):
                if lst_msg_id[j] == lst_chn_id[i]:
                    df_msg_xls.loc[j, element] = df_msgset_xls.loc[i, element]

    return df_msg_xls


# convert xls syntax to python syntax
def xls2py(dataframe):
    for i in dataframe.index.values:
        for element in list(dataframe):
            string = dataframe.loc[i, element]
            # print(string)
            if type(string) == str:
                # 1.1 operators
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('==', '=')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('=', '==')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('>==', '>=')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('!==', '!=')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('<==', '<=')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace(' div ', '/')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace(' mod ', '%')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('\\n', '\n')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace("'${", "'col{")
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('${', 'var{')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('""', str(np.NaN))
                dataframe.loc[i, element] = dataframe.loc[i, element].replace("''", str(np.NaN))

                # 1.2. functions
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('string-length', 'string_length')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('selected-at', 'selected_at')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('count-selected', 'count_selected')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('if(', 'IF(')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('date-time', 'date_time')
                dataframe.loc[i, element] = dataframe.loc[i, element].replace('format-date-time', 'format_date_time')

                # print('00 - %s: %s'%(element,dataframe.loc[i,element]))
                dataframe.loc[i, element] = dataframe.loc[i, element].replace(dataframe.loc[i, element],
                                                                              format_funcstr(dataframe.loc[i, element],
                                                                                             'jr:choice-name'))
                '''
                try:
                    dataframe.loc[i,element] = dataframe.loc[i,element].replace(dataframe.loc[i,element], format_funcstr(dataframe.loc[i,element], 'jr:choice-name'))
                except Exception as err:
                    print(err)
                '''
                # print('01 - %s: %s'%(element,dataframe.loc[i,element]))

    return dataframe


def read_messages_worksheet(google_sheet):
    try:
        ws_msg = google_sheet.worksheet('messages')  # Open the worksheet named messages
        ws_msgset = google_sheet.worksheet('messages_settings')  # Open the worksheet named messages_settings

        df_msg_xls = get_as_dataframe(ws_msg).dropna(how='all')
        df_msg_xls = df_msg_xls.dropna(subset=['channel_id'])

        df_msgset_xls = get_as_dataframe(ws_msgset).dropna(how='all')

        # merge data from two worksheets according to channel_id into one dataframe
        df_msg = xls2py(xls_merge_ws(df_msg_xls, df_msgset_xls))

        for idx in df_msg.index.values:
            df_msg.loc[idx, 'message_relevance'] = df_msg.loc[idx, 'message_relevance'].replace("''", str(np.NaN))
            df_msg.loc[idx, 'message_relevance'] = df_msg.loc[idx, 'message_relevance'].replace('""', str(np.NaN))

    except Exception as err:
        err_msg = {"error": "Reading Worsheets", "message": str(err), "code": 'ERR-GS-WSMSG'}
        logger.exception("Could not read messages worksheet")
        slack_post(err_chnl, err_msg)
        return err_msg
    return df_msg


def get_incentives(google_sheet):
    # a(ii) read incentives_settings worksheet
    try:
        # Open the worksheet named incentives_settings
        ws_incentives = google_sheet.worksheet('incentives_settings')
        df_incentives_xls = get_as_dataframe(ws_incentives).dropna(how='all')
        # convert xls syntax to python syntax
        df_incentive = xls2py(df_incentives_xls)
    except:
        logger.exception("Could not retrieve google sheet incentives")
        df_incentive = None
    return df_incentive


def open_google_sheet(google_sheet_url):
    # Type of action
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    # Authentication for Google Docs
    store = file.Storage(make_relative_dir('data', 'authentication', 'google', 'token.json'))
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(make_relative_dir('data', 'authentication', 'google', 'credentials.json'),
                                              SCOPES)
        creds = tools.run_flow(flow, store)
    gc = gspread.authorize(creds)

    # Open Google sheet and relevant worksheets
    google_sheet = gc.open_by_url(google_sheet_url)  # Open google sheet by URL

    return google_sheet


# temp pass in slack client until we move to a class here TODO: Move to class
def dct_xls_data(google_sheet_url, slack_client: PykapaSlackClient):
    logger.info("\nReading Google Sheet...")
    # 1. open worksheets in google sheet
    try:
        google_sheet = open_google_sheet(google_sheet_url)  # Open google sheet
    except Exception as err:

        err_msg = {"error": "Opening Google Sheet", "message": str(err), "code": 'ERR-GS-OPN'}
        logger.info(err_msg)
        slack_client.post_error(err_msg)
        # slack_post(config_name, err_msg)
        return err_msg

    # read compuslory sheets
    try:
        ws_svy = google_sheet.worksheet('survey')  # Open the worksheet named survey
        ws_choices = google_sheet.worksheet('choices')  # Open the worksheet named choices
        ws_set = google_sheet.worksheet('settings')  # Open the worksheet named settings

        df_svy = get_as_dataframe(ws_svy).dropna(how='all')
        df_choices = get_as_dataframe(ws_choices).dropna(how='all')
        df_set_xls = get_as_dataframe(ws_set).dropna(how='all')

    except Exception as err:

        err_msg = {"error": "Reading Worsheets", "message": str(err), "code": 'ERR-GS-WSCOMP'}
        logger.exception("Could not read compulsory sheets")
        slack_client.post_error(err_msg)
        return err_msg

    df_incentives = get_incentives(google_sheet)

    # a(i) read messages and messages_settings worksheets

    df_msg = read_messages_worksheet(google_sheet)

        # 3. Rename choices header
    old_header = df_choices.columns  # list of old headers
    new_header = {}  # empty dictionary
    for header in old_header:
        new_header[header] = 'choice_' + header  # append 'choice_' to the old header
    df_choices.rename(columns=new_header, inplace=True)  # rename headers

    # 4. split type into two variables (type and list_name)
    # df_svy = df_svy.replace(np.NaN, str(np.NaN)) # convert NaN from integer to string, i.e NaN to nan
    # a. add new column (list_name)
    # df_svy['list_name'] = np.NaN

    # b. assign type to list_name
    for i in df_svy.index.values:
        vec = df_svy.loc[i, 'type'].split()  # form vector of strings
        df_svy.loc[i, 'type'] = vec[0]  # assign first string in vector to type
        if len(vec) != 1:
            df_svy.loc[i, 'list_name'] = vec[1]  # assign second string to list_name

    # create dataframe  to reference types and dashboard state
    sel_cols = ['type', 'list_name', 'name', 'dashboard_state']
    try:
        df_select = df_svy[sel_cols]
    except:
        df_select = df_svy[sel_cols[0:3]]

    # 5. obtain form_id of worksheet
    form_id = df_set_xls.loc[0, 'form_id']

    # create dashboard header
    try:
        df_sel = df_select.dropna(subset=['dashboard_state']).astype(str)
        db_sel = df_sel[(df_sel['dashboard_state'] == 'TRUE') | (df_sel['dashboard_state'] == 'True') | (
                df_sel['dashboard_state'] == '1.0')]  # dashboard dataframe
        df_msg1 = df_msg.dropna(subset=['dashboard_state']).astype(str)
        db_msg = df_msg1[(df_msg1['dashboard_state'] == 'True') | (df_msg1['dashboard_state'] == 'TRUE') | (
                df_msg1['dashboard_state'] == '1.0') | (
                                 df_msg1['dashboard_state'] == 'DUPLICATE')]  # dashboard dataframe

        db_head = list(dict.fromkeys(list(db_msg['name']) + list(db_sel['name'])))
    except Exception as err:
        logger.info('ERROR: ', err)
        db_head = None

    # create dashboard dataframe and create worksheet
    if db_head is not None:
        db = pd.DataFrame(columns=db_head)
    else:
        db = None

    return {'messages': df_msg, 'incentives': df_incentives, 'form_id': form_id, 'select': df_select,
            'choices': df_choices, 'dashboard': db}