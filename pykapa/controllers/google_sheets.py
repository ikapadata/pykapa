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
from pykapa.settings.config import PROJECTNAME_ENV_KEY, Config
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


class StorageController:

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.data_path = self.make_relative_path(["data"])
        self.google_oauth_path = self.make_relative_path(self.base_dir, ["data", "authentication", "google", "token.json"])

    @staticmethod
    def make_relative_path(base_dir, relative_to_base: list):
        abs_file_path = os.path.join(base_dir, *relative_to_base)
        return abs_file_path


class GoogleSheetController():

    def __init__(self, slack_client: PykapaSlackClient, config: Config):
        self.config = config
        self.slack_client = slack_client
        self.scope = 'https://www.googleapis.com/auth/spreadsheets'
        self.sheet = None

    def open(self):
        self.sheet = self.open_sheet()

    def open_sheet(self):
        try:
            store = file.Storage(self.config.google_credentials)
            creds = store.get()
            if not creds or creds.invalid:
                flow = client.flow_from_clientsecrets(self.config.google_credentials, self.scope)
                creds = tools.run_flow(flow, store)
            gc = gspread.authorize(creds)
            google_sheet = gc.open_by_url(self.config.google_sheet_url)
            return google_sheet

        except Exception as err:
            err_msg = {"error": "Opening Google Sheet", "message": str(err), "code": 'ERR-GS-OPN'}
            logger.info(err_msg)
            self.slack_client.post_error(err_msg)
            raise


class PykapaGoogleSheetController(GoogleSheetController):

    def __init__(self, slack_client, config: Config):
        super().__init__(slack_client, config)
        self.open_sheet()
        self.survey = self.get_survey(self.sheet.worksheet('survey'))
        self.choices = self.rename_choices(self.sheet.worksheet('choices'))
        self.settings = self.as_dataframe(self.sheet.worksheet('settings'))
        self.incentives = self.get_incentives(self.sheet('incentives_settings'))
        self.messages = self.read_messages_worksheet_with_merge(self.sheet.worksheet('messages'), self.sheet.worksheet("messages_settings"))

    def as_dataframe(self, wsheet):
        return get_as_dataframe(wsheet).dropna(how='all')

    def rename_choices(self, wsheet):
        df_choices = self.as_dataframe(wsheet)
        new_header = {}
        for header in df_choices.columns:
            new_header[header] = 'choice_' + header
        df_choices.rename(columns=new_header, inplace=True)  # rename headers
        return df_choices

    def get_survey(self, wsheet):
        df_svy = self.as_dataframe(wsheet)
        for i in df_svy.index.values:
            vec = df_svy.loc[i, 'type'].split()  # form vector of strings
            df_svy.loc[i, 'type'] = vec[0]  # assign first string in vector to type
            if len(vec) != 1:
                df_svy.loc[i, 'list_name'] = vec[1]  # assign second string to list_name
        return df_svy

    def get_incentives(self, wsheet):
        try:
            df_incentives_xls = self.as_dataframe(wsheet)
            df_incentive = xls2py(df_incentives_xls)
        except:
            logger.exception("Could not retrieve google sheet incentives")
            self.slack_client.post_error("Could not retrieve incentives")
            df_incentive = None
        return df_incentive

    def read_messages_worksheet_with_merge(self, messages, message_settings):
        try:
            df_msg_xls = self.as_dataframe(messages).dropna(subset=['channel_id'])
            df_msgset_xls = self.as_dataframe(message_settings)

            # merge data from two worksheets according to channel_id into one dataframe
            df_msg = xls2py(xls_merge_ws(df_msg_xls, df_msgset_xls))

            for idx in df_msg.index.values:
                df_msg.loc[idx, 'message_relevance'] = df_msg.loc[idx, 'message_relevance'].replace("''", str(np.NaN))
                df_msg.loc[idx, 'message_relevance'] = df_msg.loc[idx, 'message_relevance'].replace('""', str(np.NaN))

        except Exception as err:
            err_msg = {"error": "Reading Worsheets", "message": str(err), "code": 'ERR-GS-WSMSG'}
            logger.exception("Could not read messages worksheet")
            self.slack_client.post_error(err_msg)
            return None
        return df_msg

    def make_select(self):
        sel_cols = ['type', 'list_name', 'name', 'dashboard_state']
        try:
            df_select = self.survey[sel_cols]
        except:
            df_select = self.survey[sel_cols[0:3]]
        return df_select

    def make_dashboard(self, select):
        try:
            df_sel = select.dropna(subset=['dashboard_state']).astype(str)
            db_sel = df_sel[(df_sel['dashboard_state'] == 'TRUE') | (df_sel['dashboard_state'] == 'True') | ( df_sel['dashboard_state'] == '1.0')]  # dashboard dataframe

            df_msg1 = self.messages.dropna(subset=['dashboard_state']).astype(str)
            db_msg = df_msg1[(df_msg1['dashboard_state'] == 'True') | (df_msg1['dashboard_state'] == 'TRUE') | ( df_msg1['dashboard_state'] == '1.0') | ( df_msg1['dashboard_state'] == 'DUPLICATE')]  # dashboard dataframe

            db_head = list(dict.fromkeys(list(db_msg['name']) + list(db_sel['name'])))
        except Exception as err:
            logger.info('ERROR: ', err)
            db_head = None

        # create dashboard dataframe and create worksheet
        if db_head is not None:
            dashboard = pd.DataFrame(columns=db_head)
        else:
            dashboard = None

        return dashboard


def dct_xls_data(config, slack_client: PykapaSlackClient):
    logger.info("\nReading Google Sheet...")
    sheet = PykapaGoogleSheetController(slack_client, config)
    df_select = sheet.make_select()
    dashboard = sheet.make_dashboard(df_select)
    form_id = sheet.settings.loc[0, 'form_id']
    return {'messages': sheet.messages, 'incentives': sheet.incentives,
            'form_id': form_id, 'select': df_select,
            'choices': sheet.choices, 'dashboard': dashboard}