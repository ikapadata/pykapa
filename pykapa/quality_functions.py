import json
import gspread
import os
import os.path
import json
import requests
import pytz
import numpy as np
import pandas as pd
import re
import unicodedata

from requests.auth import HTTPDigestAuth, HTTPBasicAuth
from xls_functions import *
from incentives_functions import *
from gen_funcs import *
import slackclient
from oauth2client import file, client, tools
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time
from drop_box import *
#--------------------------------------------------------------------------------------------------------------------------------
#                                                      XLS Form functions
#--------------------------------------------------------------------------------------------------------------------------------

# convert worksheet into dataFrame
def df_worksheet(worksheet):
    return pd.DataFrame(worksheet.get_all_records())

# open google sheet from specified url string
def open_google_sheet(google_sheet_url): 
    # Type of action
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    # Authentication for Google Docs
    store = file.Storage(make_relative_dir('data','authentication','google','token.json'))
    creds = store.get() 
    
    if not creds or creds.invalid:
        flow  = client.flow_from_clientsecrets(make_relative_dir('data','authentication','google','credentials.json'), SCOPES)
        creds = tools.run_flow(flow, store)
    gc = gspread.authorize(creds)

    # Open Google sheet and relevant worksheets
    google_sheet = gc.open_by_url(google_sheet_url)             # Open google sheet by URL
    
    return google_sheet
    
# remove common columns in the two data sets
def lst_reduce(df_msg_xls,df_msgset_xls):
    # list of headers
    lst_msg = list(df_msg_xls) 
    lst_msgset = list(df_msgset_xls)
    
    for element in lst_msg:
        if element in lst_msgset:
            lst_msgset.remove(element)
        
    return lst_msgset

# append columns
def append_col(dataframe,header):
    if type(header) == list:
        for element in header:
            dataframe[element] = ''
    elif header.replace(' ','') == '':
        dataframe = dataframe
    else:
        dataframe[header] = ''
        
    return dataframe


# merge data from two worksheets according to channel_id into one dataframe
def xls_merge_ws(df_msg_xls,df_msgset_xls):
    lst_msg_id = df_msg_xls.loc[:,'channel_id']            # list of message IDs in messages worksheet
    lst_chn_id = df_msgset_xls.loc[:,'channel_id']         # list of message IDs in messages_settings worksheet
    
    lst_headers = lst_reduce(df_msg_xls,df_msgset_xls)     # list of headers to append
    
    # append headers
    df_msg_xls = append_col(df_msg_xls,lst_headers)
    # merge the two datasets into one dataframe
    for element in lst_headers:
        for i in range(len(lst_chn_id)):
            for j in range(len(lst_msg_id)):
                if lst_msg_id[j] == lst_chn_id[i]:
                    df_msg_xls.loc[j,element] = df_msgset_xls.loc[i,element]
    
    return df_msg_xls
                  
# convert xls syntax to python syntax
def xls2py(dataframe):
    for i in dataframe.index.values:
        for element in list(dataframe):
            string = dataframe.loc[i,element]
            #print(string)
            if type(string) == str:
                # 1.1 operators
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('==','=')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('=','==')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('>==','>=')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('!==','!=')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('<==','<=')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace(' div ','/')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace(' mod ','%')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('\\n','\n')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace("'${","'col{")
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('${','var{')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('""',str(np.NaN))
                dataframe.loc[i,element] = dataframe.loc[i,element].replace("''",str(np.NaN))

                #1.2. functions
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('string-length','string_length')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('selected-at','selected_at')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('count-selected','count_selected')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('if(','IF(')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('format-date-time(','format_date_time(')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('date-time','date_time')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('format-date-time(','format_date_time(')
                
                #print('00 - %s: %s'%(element,dataframe.loc[i,element]))
                dataframe.loc[i,element] = dataframe.loc[i,element].replace(dataframe.loc[i,element], format_funcstr(dataframe.loc[i,element], 'jr:choice-name'))
                '''
                try:
                    dataframe.loc[i,element] = dataframe.loc[i,element].replace(dataframe.loc[i,element], format_funcstr(dataframe.loc[i,element], 'jr:choice-name'))
                except Exception as err:
                    print(err)
                '''
                #print('01 - %s: %s'%(element,dataframe.loc[i,element]))
    
    return dataframe


# retrieve relevant data from google sheet
def dct_xls_data(google_sheet_url, err_chnl = None):
    print("\nReading Google Sheet...")
    # 1. open worksheets in google sheet
    try:
        google_sheet = open_google_sheet(google_sheet_url)                  # Open google sheet
    except Exception as err:
        print("\ntypeError: %s"%type(err))
        err_msg = "*GOOGLE SHEET ERROR:* \n `Warning: Failed to ropen the Google Sheet.` \n*Google Sheet: *%s `ERROR [%s]`: %s" %(google_sheet_url,err['code'],err['message'])
        print(err_msg)
        slack_post(err_chnl,err_msg)
        return err_msg
    
    # Read the survey worksheet
    try:
        ws_svy =  google_sheet.worksheet('survey')                   # Open the worksheet
        df_svy = get_as_dataframe(ws_svy).dropna(how = 'all')
            
    except Exception as err:
        print("\ntypeError: %s"%type(err))
        err_msg = "*GOOGLE SHEET ERROR:* \n `Warning: Failed to read survey worksheet.` \n*Google Sheet: *%s `ERROR [%s]`: %s" %(google_sheet_url,err['code'],err['message'])
        print(err_msg)
        slack_post(err_chnl,err_msg)
        return err_msg
    
    # Read the choices worksheet
    try:
        ws_choices =  google_sheet.worksheet('choices')                   # Open the workshee
        df_choices = get_as_dataframe(ws_choices).dropna(how = 'all')
        # Rename choices header
        old_header = df_choices.columns # list of old headers
        new_header = {}                 # empty dictionary
        for header in old_header:
            new_header[header] = 'choice_'+ header # append 'choice_' to the old header
        df_choices.rename(columns = new_header, inplace=True) # rename headers
    
    except Exception as err:
        print("\ntypeError: %s"%type(err))
        err_msg = "*GOOGLE SHEET ERROR:* \n `Warning: Failed to read choices worksheet.` \n*Google Sheet: *%s `ERROR [%s]`: %s" %(google_sheet_url,err['code'],err['message'])
        print(err_msg)
        slack_post(err_chnl,err_msg)
        return err_msg
    
    # Read the settings worksheet
    try:
        ws_set = google_sheet.worksheet('settings')                   # Open the workshee
        df_set_xls = get_as_dataframe(ws_set).dropna(how = 'all')
        # obtain form_id of worksheet
        form_id = df_set_xls.loc[0,'form_id']
    except Exception as err:
        print("\ntypeError: %s"%type(err))
        err_msg = "*GOOGLE SHEET ERROR:* \n `Warning: Failed to read choices worksheet.` \n*Google Sheet: *%s `ERROR [%s]`: %s" %(google_sheet_url,err['code'],err['message'])
        print(err_msg)
        slack_post(err_chnl,err_msg)
        return err_msg
    
    
    # Read the messages worksheet
    try:
        ws_msg = google_sheet.worksheet('messages')                   # Open the workshee
        df_msg_xls = get_as_dataframe(ws_msg).dropna(how='all')
        df_flt_msg = df_msg_xls[(df_msg_xls['channel_id'].isnull()) | (df_msg_xls['message_relevance'].isnull())]
    except Exception as err:
        print("\ntypeError: %s"%type(err))
        err_msg = "*GOOGLE SHEET ERROR:* \n `Warning: Failed to read messages worksheet.` \n*Google Sheet: *%s `ERROR [%s]`: %s" %(google_sheet_url,err['code'],err['message'])
        print(err_msg)
        slack_post(err_chnl,err_msg)
        return err_msg
    
    # Read the messages_settings worksheet
    try:
        ws_msgset = google_sheet.worksheet('messages_settings')                   # Open the workshee
        df_msgset_xls = get_as_dataframe(ws_msgset).dropna(how='all')
        df_flt_msgset = df_msgset_xls[(df_msgset_xls['channel_id'].isnull()) | (df_msgset_xls['channel_name'].isnull()) | (df_msgset_xls['messenger'].isnull())]
     
    except Exception as err:
        print("\ntypeError: %s"%type(err))
        err_msg = "*GOOGLE SHEET ERROR:* \n `Warning: Failed to read messages_settings worksheet.` \n*Google Sheet: *%s `ERROR [%s]`: %s" %(google_sheet_url,err['code'],err['message'])
        print(err_msg)
        slack_post(err_chnl,err_msg)
        return err_msg

    try:
        if not df_flt_msg.empty:
            #print('START 3.a')
            idx = list(df_flt_msg.index.values)
            for i in range(len(idx)):
                idx[i] = idx[i] + 2    
                alert_msg = "\n `ERROR`: In the *messages*  worksheet row(s) %s have missing required fields. Remember to always provide the appropriate channel_id, message relevance, and  message_label for alerts to be sent." %idx
        else:
            alert_msg = ""
                
        if not df_flt_msgset.empty:
            idx = list(df_flt_msgset.index.values)
            for i in range(len(idx)):
                idx[i] = idx[i] + 2       
                alert_set = "\n`ERROR`: In the *messages_settings* worksheet row(s) %s have missing required fields. Remember to always provide the appropriate channel_id, channel_name, and messenger for alerts to be sent." %idx
        else:
            alert_set = ""
        
        # combine alert messages
        alert = alert_msg + alert_set
                
        if alert != "":
            alert_slk = '*INVALID ALERT SETTINGS:* \n`WARNING: No alerts will be sent because of the following reasons.`\n*Google Sheet:* %s \n%s'%(google_sheet_url,alert)
                    
            slack_post(err_chnl,alert_slk)
            print(alert_slk)
                    
            return alert_slk
                
    except Exception as err:
        print('Required Messages Fields Exception: %s' %err)
                

    df_msg = xls2py(xls_merge_ws(df_msg_xls,df_msgset_xls))

    for idx in df_msg.index.values:
        df_msg.loc[idx,'message_relevance'] = str(df_msg.loc[idx,'message_relevance']).replace("''",str(np.NaN))
        df_msg.loc[idx,'message_relevance'] = str(df_msg.loc[idx,'message_relevance']).replace('""',str(np.NaN))
    
    # Read incentives_settings worksheet   
    try:
        try:
            ws_incentives =  google_sheet.worksheet('incentives_settings')  # Open the worksheet named incentives_settings
            df_incentives_xls = get_as_dataframe(ws_incentives).dropna(how='all')
        except Exception as err:
            err_msg = "\nIncentives Exception: %s" %err
            print(err_msg)
            
        df_incentive = xls2py(df_incentives_xls)
        
        # return rows with empty required fields
        df_flt_incntives = df_incentives_xls[(df_incentives_xls['incentive_type'].isnull()) | (df_incentives_xls['amount'].isnull()) | (df_incentives_xls['contact'].isnull()) | (df_incentives_xls['network'].isnull()) | (df_incentives_xls['recharge_count'].isnull()) | (df_incentives_xls['relevance'].isnull()) | (df_incentives_xls['flickswitch_api_key'].isnull())]
            
        if not df_flt_incntives.empty:
            idx = list(df_flt_incntives.index.values)
            for i in range(len(idx)):
                idx[i] = idx[i] + 2       
                alert = "*INVALID INCENTIVES SETTINGS*\n `Warning: Incentives will not be sent because of the following reasons` \n*Google Sheet: * %s \n`ERROR`: In the *incentives_settings* worksheet row(s) %s have missing required fields. Remember all of the fields are required and have to be filled for incentives to be sent." %(google_sheet_url, idx)
                print(alert)
                slack_post(err_chnl, alert)
    except:
        df_incentive = None 
    
    # assign type to list_name 
    for i in df_svy.dropna(subset=["type"]).index.values:
        vec = df_svy.loc[i,'type'].split() # form vector of strings
        df_svy.loc[i,'type'] = vec[0] # assign first string in vector to type
        if len(vec)!=1:
            df_svy.loc[i,'list_name'] = vec[1] # assign second string to list_name
                
    #create dataframe  to reference types and exports
    sel_cols = ['type','list_name','name','pykapa_export']
    try:
        df_select = df_svy[sel_cols]
    except:
        df_select = df_svy[sel_cols[0:3]]
    
    # Read pykapa_export worksheet
    try:
        try:
            ws_xpt = google_sheet.worksheet('pykapa_export')
            df_xpt = get_as_dataframe(ws_xpt).dropna(how = 'all')
        except Exception as err:
            err_msg = "\nExport Exception: %s" %err
            df_xpt = None
        
        if df_xpt is not None:
            # Find missing
            
            df_dbx = df_xpt[(df_xpt['channel'] == 'dropbox')]
           
            if not df_dbx.empty:
                df_rows = df_dbx.dropna(how='any', axis=0, subset = ["fields","api_key","path","view"])
                if len(df_dbx) != len(df_rows):
                    alert_dbx = "\n`ERROR`: Won't export to Dropbox because of missing required fields. For the *dropbox* channel, please specify the relevant values for fields, api_key, path, and view."
                else:
                    alert_dbx = ""
            else:
                print(None)
                alert_dbx = ""
                
            df_atbl = df_xpt[(df_xpt['channel'] == 'airtable')]
            
            if not df_atbl.empty:
                df_rows = df_atbl.dropna(how='any', axis=0, subset =["fields","name","api_key","path","view"])
                if len(df_atbl) != len(df_rows):
                    alert_atbl = "\n`ERROR`: Won't export to Airtable because of missing required fields. For the *airtable* channel, please specify the relevant values for fields, name, api_key, path, and view."
                else:
                    alert_atbl = ""
            else:
                print(None)
                alert_atbl = ""
                
            df_def = df_xpt[(df_xpt['channel'] == 'formdef')]
            
            if not df_def.empty:
                df_rows = df_def.dropna(subset = ["fields","name"])
                if len(df_def) != len(df_rows):
                    alert_def = "\n`ERROR`: Won't export to Google Sheet because of missing required fields. For the *formdef* channel, please specify the relevant values for fields, and name."
                else:
                    alert_def = ""
            else:
                print(None)
                alert_def = ""
         
            alert_xpt = alert_def + alert_dbx + alert_atbl
            
            if alert_xpt != "":
                alert_slk = "*INVALID EXPORT SETTINGS:* \n`Warning: Failed Exports.`\n*Google Sheet: * %s \n%s" %(google_sheet_url, alert_xpt)
                slack_post(err_chnl, alert_slk)
                print(alert_slk)
        else:
            print(err_msg)
        
    except Exception as err:
        err_msg = {"error": "Export Worksheet Exception","message": str(err),"code":'PYXPT'}
        print(err_msg)
        #slack_post(err_chnl,err_msg)
        
    # FORMDEF FIELDS
    try:
        db_head_svy = export_fields(df_select, export_type = 'formdef', export_field = 'pykapa_export')
    except:
        db_head_svy = []
            
    try:
        db_head_msg = export_fields(df_msg, export_type = 'formdef', export_field = 'pykapa_export')
    except:
        db_head_msg = []
              
    db_head_all = list(dict.fromkeys( db_head_msg + db_head_svy))
        
    #print('\nDASHBOARD HEADER (1): %s'%db_head_all)

    if db_head_all != []:
    
        df_db = pd.DataFrame(columns = db_head_all)
        #print('\nDB : \n',df_db)
    else:
        df_db = None
            
        
    filepath = './data/projects/%s/qctrack.json'%form_id
    data = {'StartDate': '','CompletionDate':'','KEY':'', 'GoogleSheet':'','failedRecharges': []}
        
    if os.path.isfile(filepath):
        qctrack = read_json_file(filepath)
        qctrack['GoogleSheet'] = google_sheet_url
        print('gSheet: %s'%google_sheet_url)
        write_to_json(filepath, qctrack)
    else:
        data['GoogleSheet'] = google_sheet_url
        write_to_json(filepath, data)
      
    return {'messages': df_msg, 'incentives':df_incentive, 'form_id':form_id, 'select':df_select, 'choices':df_choices, 'export':df_xpt,'dashboard':df_db}

# relative directory to script
def relative_dir(folder, file):

    script_path = os.path.abspath('__file__') # path to current script
    script_dir = os.path.split(script_path)[0] #i.e. /path/to/dir/
    abs_file_path = os.path.join(script_dir, folder, file)
    
    return abs_file_path

# write data frame to csv
def data_to_csv(df_data,filename, folder):
    #create directory and filepath
    dir_csv = make_relative_dir( 'data', folder,'') #create directory
    path_csv= concat(dir_csv,filename,'.csv') # create path file
    # create directory if it doesn't exist
    if not os.path.exists(dir_csv):
        os.makedirs(dir_csv)
        
    #write or append data to csv
    if os.path.exists(path_csv) == False:
        df_data.to_csv(path_csv, sep='\t', index=False) # write to csv
    else: 
        df_data.to_csv(path_csv, mode='a', sep='\t', index=False, header= False) # append to csv
        

# create a link to the surveyCTO json file
def surveyCTO_link(server,form_id, dirX):

    # open file if it exists
    if os.path.isfile(dirX) == True:
        with open(dirX) as jsonX:
            json_file = json.load(jsonX)
        # check if data in json file
        
        completionDate = json_file['CompletionDate']
        
        if completionDate != '':
            timestamp = int(date_time(completionDate).timestamp()*1000)
            # survey url
            svy_url = server + '/api/v1/forms/data/wide/json/'+form_id + '?date='+str(timestamp)
        
        else:
            #timestamp = int(date_N_days_ago(365).timestamp()*1000)
            svy_url = server + '/api/v1/forms/data/wide/json/'+form_id #+ '?date='+str(timestamp)
            
    else:
        #f = open(dirX,"w+")
        #json.dump({'StartDate': '','CompletionDate':'','KEY':'', 'GoogleSheet':'','failedRecharges': []}, f) # write to json file
        #f.close()
        
        #timestamp = int(date_N_days_ago(365).timestamp()*1000)
        svy_url = server + '/api/v1/forms/data/wide/json/'+form_id #+ '?date='+str(timestamp)
            
    print('surveyCTO link: ', svy_url)
    #print(dirX)
            
            
    return svy_url


def surveyCTO_response(server,username,password,form_id):
    Dir = "./data/projects/%s" % form_id
    dirX = "./data/projects/%s/qctrack.json" % form_id
    print('dirTrack: ',dirX)
    if not os.path.exists(Dir):
        os.makedirs(Dir)
    #create surveyCTO link
    file_url = surveyCTO_link(server,form_id, dirX)
    
    # download json file from surveyCTO
    print('\nRequesting data from surveyCTO')
    resp = requests.get(file_url, auth=HTTPBasicAuth(username, password))
    
    return resp

# download data from surveyCTO
def surveyCTO_download(server,username,password,form_id, err_chnl = None):
    
    resp = surveyCTO_response(server,username,password,form_id)
    status = resp.status_code
    
    date = timezone_sast(str(now()))
    stat_msg = "*DATA DOWNLOAD:* \n_Form ID_: %s \n_Status_: %s [%s] \n_Date_: %s" %(form_id,status, resp.reason, date)
    
    
    slack_post(str(err_chnl),stat_msg)
    print(stat_msg)
    
    try:
        json_data = resp.json()
        # convert json to dataframe
        df_json = pd.DataFrame(json_data)[list(json_data[0])]
    except:
        df_json = pd.DataFrame()
    
    # send messgaes to error channel if download is unsuccessful
    if status == 401:
        err = '`SurveyCTO Error: (' + str(status) + ')`\n' + "The credentials you've entered are invalid. Please enter the correct credentials to continue."
        slack_post(str(err_chnl),err) # post message on slack  
        return None
    
    elif status == 404:
        #err = '`SurveyCTO Error: (' + str(status) + ')`\n' + "The server ("+server+" ) you were trying to reach couldn't be found."
        #slack_post(str(err_chnl),err) # post message on slack
        return pd.DataFrame()
    
    elif status != 200 and status != 401 and df_json.empty == False:
        err = '`SurveyCTO Error: (' + str(status) + ')`\n' + df_json.loc['message','error']
        slack_post(str(err_chnl),err) # post message on slack
         
    if len(list(df_json)) > 1:
        # convert string to datetime
        df_json['CompletionDate'] = pd.to_datetime(df_json['CompletionDate'])
        df_json = df_json.sort_values(by=['CompletionDate']) # sort values in ascending order of CompletionDate

    return df_json.replace('', str(np.NaN))



# determine all columns necessary for quality control
def qc_fields(dct_xls):
    cols = ['CompletionDate','KEY']
    for key in dct_xls:
        print(key)
        df = dct_xls[key]
        if df is not None and (key == 'messages' or key == 'incentives'):

            for col in df.columns:
                for i in df.index.values:
                    string = df.at[i,col]
                    
                    occ = str(string).count('{') # count the occurance of a substring in string
                    for i in range(occ):
                        field = get_column(string,'{') # get column from string
                        #print('field: %s | string: %s'%(field,string))
                        cols.append(str(field).strip()) # add column
                        
                        if col is not None:
                            string = string.replace('{'+field+'}','')
                            occx = str(string).count('{')

                            if occx == 0:
                                break
                            
        if key == 'dashboard' and df is not None:
            db_cols = list(dct_xls['dashboard'])
        else:
            db_cols = []
        
        qc_cols = cols + db_cols
        
    return list(dict.fromkeys(qc_cols))

# get column name referenced in xls formatted string
def get_column(string, id_str):
    l = len(id_str)
    
    f_idx   = string.index(id_str) # index of the id_string
    new_str = string[f_idx: ] # new short string
    char_i = 0 # initialize char counter
    open_list = ["[","{","("]
    # iterate through the string to find the id_strtion
    if new_str[l-1] in open_list:   
        for char in new_str:
            char_i +=1
            # check if there are balanced parantheses
            bal_par = balanced_par(new_str[0:char_i])
            
            if bal_par == True:
                
                col = new_str[l:char_i-1]
                return col

#function to post messages to slack
def slack_post(channel_name, slack_msg): 
    json_bot = read_json_file('./data/authentication/slack/bot_token.json')
    bot_token = json_bot['BOT_TOKEN']
    slack_client = slackclient.SlackClient(bot_token)
    try:
        slk_txt = slack_user_id(slack_msg)
    except Exception as err:
        print('Slack User Exception: %s'%err)
        slk_txt  = slack_msg
    slack_client.api_call('chat.postMessage', channel=str(channel_name).lower(), text=slk_txt)
    #slack_client.chat_postMessage( channel = channel_name, text = slack_msg)
    
#get user id to send in messages
def slack_user_id(string):
    json_bot = read_json_file('./data/authentication/slack/bot_token.json')
    bot_token = json_bot['BOT_TOKEN']
    sc = slackclient.SlackClient(bot_token)
    
    users = sc.api_call("users.list")['members']
    df =  pd.DataFrame(users)
    for i in df.index.values:
        string = string.replace("<@%s>"%df.at[i,'name'],"<@%s>"%df.at[i,'id'])
        
    return string
#--------------------------------------------------------------------------------------------------------------------------------
#                                                      Quality Control functions
#--------------------------------------------------------------------------------------------------------------------------------

# evaluate conditions and post messages to channel    
def control_messenger(df_msg,dct_xls,err_chnl=None,df_dashboard=None,google_sheet_url=None,form_id=None ):
    # list of unique channel names
    df_msg = df_msg.sort_values(by=['channel_id'])
    channels = df_msg.drop_duplicates(subset='channel_name', keep ='first')['channel_name']
    nan = 'nan' 
    
    for chnl in channels:
        msg = ''               # initialize
        error = ''             # initialize
        
        dfx = df_msg.loc[df_msg['channel_name'] == chnl] # filter by channel name
        print(dfx.at[0,'name'])
        
        
        for index in dfx.index.values:
            index = int(index)
            # get the cell values in the messages sheet
            msg_label = evalfunc_str(str(dfx.loc[index,'message_label']).replace('""',''),dct_xls) # message label to be posted on messenger app
            msg_rel = dfx.loc[index,'message_relevance'] # message relevance to trigger message label
            header = evalfunc_str(dfx.loc[index,'message_header'],dct_xls) # message header to appear above the message labels
            messenger = str(dfx.loc[index,'messenger']).lower() # name of messenger app
            
            alert_name = dfx.at[index,'name']
            
            print("index: %s | type = %s"%(index,type(index)))
            print("1st: %s | 2nd %s"%(dfx.at[0,'name'],dfx.at[index,'name']))
         
            
            #print("\n# NAME: %s| MSGREL: %s | EVAL: %s" % (alert_name, msg_rel, eval(str(msg_rel)) ) )
            try:
                if msg_rel != 'nan' and eval(str(msg_rel))==True: # and db_state == 'TRUE':
                    msg = msg + msg_label + '\n'
                    value = 1
                    # populate dashboard
                    df_dashboard = dashboard(df_dashboard,alert_name,value)
                    
            except Exception as exceptErr:
                index += 2
                msg_rel = msg_rel.replace('var{','${').replace("col{","${")
                err = '\n`row:` '+str(index) + '\t `message relevance:` '+ str(msg_rel).replace('==','=')+ '\t `error message:` ' + str(exceptErr)
                error = concat(error, err)
        
        chnl = chnl.replace('""','')
        chnl = chnl.replace('"','').lower()
        
        # find dupicate index
 
        
        if 'DUPLICATE' in dfx['name']:
            df_dup = dfx[ dfx['name'] == 'DUPLICATE']
        else:
            df_dup = pd.DataFrame()
        
        if not df_dup.empty:
            idx =  df_dup.index.values[0]
            msg_rel_dup = df_dup.at[idx, 'message_relevance']
        else:
            msg_rel_dup = None
        
        #print('\n NEW DASHBOARD DATA: \n %s'%df_dashboard)
        #print('\n ***** Write and Determine Duplicates in Google Sheets Dashboard Worksheet ***** ')
        df_xpt = dct_xls['export']
        
        try:
            idx = df_xpt[df_xpt['channel']=='formdef'].index.values[0]
            xpt_name = df_xpt.at[idx,'name']
        except Exception as err:
            print('\nFormdef Exception: %s'%err)
            xpt_name = 'dashboard'
            
        #dup_state = to_google_sheet(df_dashboard, google_sheet_url,err_chnl=err_chnl, msg_rel = msg_rel_dup, ws_title = xpt_name )
        #print('Duplicate: ',dup_state)
        #print('\n ***** Completed Dashboard Worksheet Update ***** ')
        dup_state = False
        if dup_state == True:
            dup_msg = df_dup.at[idx, 'message_label']
            msg = msg + dup_msg + '\n'

        
        if msg !='':
            #print('channel: ',chnl,'\nmsg: \n', msg)
            print("\n# CHANNEL: %s"%chnl)
            if messenger == 'slack':
                slack_msg = header +'\n'+ msg
                slack_msg = slack_msg.replace('""','')
                slack_msg = slack_msg.replace('"','')

                print('\nMessage channel: ', chnl)
                slack_post(chnl, slack_msg.replace('nan',''))
                
            elif messenger == 'email':
                fromEmail = "whatsapp@owtcast.co.za" #input("From: ")
                fromPass = "Wh4+54pp_2020"
                smptpServer = 'smtp.owtcast.co.za'
                send_email(fromEmail,toEmail=chnl, subject = header,text=msg, fromPass=fromPass, smptpServer=smptpServer)
        
        if error !='':
            #print('channel: ',chnl,'\nmsg: \n', msg)
            if messenger == 'slack':
                print("\n# ERRCHANNEL: %s"%err_chnl)
                error = '`The alerts below are not sent because of the following syntax error(s):`\nMake sure xls names ( i.e ${name} ) in the *messages* worksheet exists, spelled correctly, or the same type is compared in the following:\n' + error
                slack_synt = header + '\n' + error + '\n' + google_sheet_url
                slack_synt = slack_synt.replace('""','')
                slack_synt = slack_synt.replace('"','')
                   
                chnl = chnl.replace('""','')
                chnl = chnl.replace('"','')
                print('\nSyntax channel: ', err_chnl)
                slack_post(err_chnl, slack_synt.replace('nan',''))
            
            elif messenger == 'email':
                fromEmail = "whatsapp@owtcast.co.za" #input("From: ")
                fromPass = "Wh4+54pp_2020"
                smptpServer = 'smtp.owtcast.co.za'
                send_email(fromEmail,toEmail=chnl, subject = header,text=msg, fromPass=fromPass, smptpServer=smptpServer)
        
    
    return df_dashboard


#convert UTC to SAST timezone
def timezone_sast(date_str):
    dt = date_time(date_str)
    fmt = '%b %d, %Y   %H:%M:%S'
    utc_dt = pytz.utc.localize(dt)
    #convert utc to jhb timezone
    jhb = pytz.timezone('Africa/Johannesburg')
    return  utc_dt.astimezone(jhb).strftime(fmt)

# quality contorl and messenger (populate xls control variables with data from surveyCTO)
def qc_messenger(df_survey,dct_xls,qc_track, admin_channel = None, google_sheet_url=None, duplicate_key = ''):
    
    form_id = dct_xls['form_id']
    dirX = make_relative_dir('data','projects', form_id, 'qctrack.json')
    
    
    if df_survey.empty == False and list(df_survey)!= ['error']:
    
        
        print('\nQC STARTED')
        df_select = dct_xls['select']
        df_choices = dct_xls['choices']
        
        
        print('recs: %s\n'%(len(df_survey)))
            
        #Convert number strings with commas in pandas DataFrame to float
        decmark_reg = re.compile('(?<=\d),(?=\d)')
        
        
        if dct_xls['messages'] is not None and dct_xls['messages'].empty == False:
            
            for i in df_survey.index.values:
    
                # sort messages in ascending order of channel IDs
                df_msg = dct_xls['messages'].sort_values('channel_id',ascending=True)
            
                df_incentive = deepcopy(dct_xls['incentives'])
                df_dashboard = deepcopy(dct_xls['dashboard'])

                
                for col in df_survey: 
                    # format values from respondent
                    val = df_survey.loc[i,col] # read value from respondent
                    # esnure all values are string
                    if type(val)!= str:
                        val = str(val)

                    value = decmark_reg.sub('.',val) # change decimal point from a comma (,) to a period (.)
                    
                    # determine column type
                    try:
                        colType = dct_xls['select'].loc[dct_xls['select'][dct_xls['select'].name == col].index.values[0],'type']
                    except:
                        colType = None
                    
                    # format value
                    if str(colType) != 'select_multiple':
                            # convert UTC to SAST timezone
                        if date_check(value)== True and is_number(value)==False:
                            value = timezone_sast(value)
                            # format strings to cater for multiple lines
                        elif is_number(value) == False:
                            value = '"""' + value + '"""'
                        elif value.isdigit():
                            value = int(value)
                    else:
                        value = '"""' + value + '"""'
   
                    # populate dashboard dataframe
                    df_dashboard = dashboard(df_dashboard,col,value,colType,dct_xls)
                    
                    
                    # populate xls variables with data from surveyCTO
                    df_msg = df_msg.replace('var{'+col+'}',str(value), regex = True) # messages
                    df_msg = df_msg.replace('col{'+col+'}',col, regex = True) # messages
                        
                    if df_incentive is not None:
                        df_incentive = df_incentive.replace('var{'+col+'}',str(value), regex = True) # incentives
                        df_incentive = df_incentive.replace('col{'+col+'}',col, regex = True) # incentives

                
                # evaluate quality control conditions and post messages on channels
                print('\n ***** Evaluate Quality Control Messages ***** ')
                df_db = control_messenger(df_msg, dct_xls, admin_channel, df_dashboard,google_sheet_url= google_sheet_url)            
                print('\n ***** Completed Evaluation Quality Control Messages ***** ')
                print(df_db)
                # ----------------- send incentives -------------
                if df_incentive is not None and df_incentive.empty == False:

                    for idx in df_incentive.index.values:
                        # recharge details
                        try:
                            msisdn  = simcontact(evalfunc_str(str(df_incentive.loc[ idx,'contact']),dct_xls))
                            #print('xlsContact: ',msisdn, ' simContact: ', simcontact(msisdn) )
                            api_key = evalfunc_str(str(df_incentive.loc[ idx,'flickswitch_api_key']),dct_xls)
                            r_count = int(float(evalfunc_str(str(df_incentive.loc[ idx,'recharge_count']), dct_xls)))
                            network = evalfunc_str(str(df_incentive.loc[ idx,'network']), dct_xls)
                            amount = evalfunc_str(str(df_incentive.loc[ idx,'amount']), dct_xls)
                            key = df_survey.loc[i,'KEY']
                            prod_type = evalfunc_str(str(df_incentive.loc[ idx,'incentive_type']), dct_xls)
                            
                            if msisdn != 'nan' and network != 'nan':
                                #msisdn = simcontact(msisdn)
                                df_rec = msisdn_history(api_key, msisdn, prodType = prod_type) # check history
                                print('\nSIM HISTORY: %s [Recharges = %s] '%(msisdn,len(df_rec)) )
                                
                                if df_rec is not None and len(df_rec)!=0 and type(df_rec)!= list:
                                    print('idx: ', idx,' type: ', prod_type,  ' msisdn: ', df_rec.loc[0,'msisdn'],' status: ',df_rec.loc[0,'status'],'\n' )
                                    #s_rec = df_rec[df_rec['reference'].str.contains(form_id) & df_rec['status'].str.contains('SUCCESS')] # records of successful recharges in the given project
                                    #f_rec = df_rec[df_rec['reference'].str.contains(form_id) & df_rec['status'].str.contains('FAILED')]  # records of FAILED recharges in the given project
                                    s_rec = df_rec[df_rec['status'].str.contains('SUCCESS')] # records of successful recharges in the given project
                                    f_rec = df_rec[df_rec['status'].str.contains('FAILED')]  # records of FAILED recharges in the given project
                                            
                                elif df_rec is None:
                                    s_rec = None
                                    f_rec = None
                                else:
                                    s_rec = []
                                    f_rec = []
                                
                                # recharge msisdn
                                if s_rec is not None and f_rec is not None:
                                    if len(s_rec) < r_count and len(f_rec) <= 1: 
                                        print('Buying %s for %s (%s)'%(prod_type, msisdn, network))
                                        
                                        recharge = rechargeSim(api_key = api_key, msisdn = msisdn , network = network, prodType = prod_type, bundleSize = amount, price = amount, ref = concat(key,'_', form_id,'_', len(f_rec)+1))
                                        
                                        if recharge is not None and recharge.empty == False:
                                            print('STATUS: ', recharge['status'],'\n')
                                            # read the tracking json file 
                                            qc_track = read_json_file(dirX)
                                            failed_msisdn = qc_track['failedRecharges'] # list of failed recharges
            
                                            if len(list(recharge))>0 and recharge.loc[0,'status'] != 'SUCCESS':
                                                failed_msisdn.append(msisdn)
                                            elif len(list(recharge))==0:
                                                failed_msisdn.append(simcontact(msisdn))
                                                        
                                            failed_msisdn = list(dict.fromkeys(failed_msisdn))
                                            
                                            qc_track['failedRecharges'] = failed_msisdn
                                  
                        except Exception as err:
                            print('\nIncentives Exception: %s' %err)
    
                # keep track of the last checked record
                date_new = format_date_time(str(df_survey.loc[i,'CompletionDate']), '%b %d, %Y   %H:%M:%S')
                qc_track['CompletionDate'] = date_new
                qc_track['KEY'] = df_survey.loc[i,'KEY']


                if qc_track['StartDate'] == '' and i == 0:
                    qc_track['StartDate'] = date_new
    
                print('\n',qc_track)
                write_to_json(dirX, qc_track) # record the last checked interview in json fileq               
                #to_google_sheet(df_dashboard = df_db, google_sheet_url = google_sheet_url)
    
    return True
                
                
                
def qc_manager(google_sheet_url,username,password,server):
    # create incentive database tables      
    excess_recharge_dir = make_relative_dir('data','db','recharges','excess_recharges.json') #.../data/db/recharges/excess_recharges.json
    create_json_db(excess_recharge_dir)

    recharge_dir = make_relative_dir('data','db','recharges','recharges.json') #.../data/db/recharges/recharges.json
    create_json_db(recharge_dir)

    while True:
        dct_xls    = dct_xls_data(google_sheet_url)  # retrieve quality control and incentive data from xls form as dataframe
        df_survey = surveyCTO_download(server,username,password,dct_xls['form_id']) # retrieve data from surveyCTO json file as dataframe
        qc_messenger(df_survey,dct_xls) # perform perform quality control, post messages, and send incentives

        import time
        print('The End')
        time.sleep(200)
        
# determine the type of a column  value      
def coltype(col, df_select):
    row     = df_select[df_select.name == col]
    coltype = row.loc[row.index.values[0],'type']
    return coltype      

# create new worksheet
def new_worksheet(google_sheet,ws_title):
    try:
        dashboardSheet = google_sheet.add_worksheet(title=ws_title, rows="1", cols="2")
        err = None
        return err
    except Exception as err:
        err = 'Worksheet already exists.'
        return err

# dashboard       
def dashboard(df_dashboard,col,value,colType= None,dct_xls=None):
    
    if df_dashboard is not None and col in list(df_dashboard):
        
        if type(value) == str:
            try:
                db_label = get_substring('"""', '"""', value)
            except:
                db_label = value

        else:
            db_label = value
            
        df_dashboard.loc[0,col] = db_label
    
        
    return df_dashboard


def to_google_sheet(df_dashboard, google_sheet_url,err_chnl = None, ws_title = 'dashboard', msg_rel = None ):
    
    # open google sheet and get worksheets
    gsheet = open_google_sheet(google_sheet_url) #  list of sheet names
    
    #print('DASHBOARD HEADER: ',list(df_dashboard))
    
    try:
        print('dashboard created')
        ws_db = gsheet.worksheet(ws_title) # open worksheet
        
        df_recs = get_as_dataframe(ws_db).dropna(how = 'all')
        index = len(df_recs) + 2
        
        print('\nindex: ',index)       
        
        
        
        if not df_recs.empty:
            print()
            
            ws_head = list(df_recs) # get headers from worksheet
            db_head = list(df_dashboard) # get columns from TRUE dashboard_state variables
           
            df_e = pd.DataFrame( columns = list( set(ws_head).difference( list(df_dashboard) ) ) )
            df_DB = pd.concat([df_e,df_dashboard], sort = False).replace(np.nan, '', regex=True)[ws_head]
            #df_DB = pd.concat([df_e,df_dashboard], sort = False).replace('nan', '', regex=True)[ws_head]            
           
            # FIND DUPLICATE
            #print('\nDASHBOARD HEADER (2): ',list(df_DB))
            print('\nMSGREL: type(%s)\n%s'%(type(msg_rel),msg_rel))
           
            if msg_rel is not None:
                try:
                    df_dup = filter_by_relevance(msg_rel,df_recs )
                    
                    if not df_dup.empty:
                        df_DB['DUPLICATE'] = 1
                        dup_state = True
                    else:
                        df_DB['DUPLICATE'] = 0
                        dup_state = False

                except Exception as err:
                    print('Exception: ', err)
                    slack_post(err_chnl, '`Duplicate Exception`: %s' % err)
                    df_DB['DUPLICATE'] = 0
                    dup_state = False
            else:
                dup_state = False
                
            
            df_DB = df_DB.astype(str)
            df_DB = df_DB.replace('nan', '', regex=True)
            # Convert data frame to a list
            
            #print('Writing To Dashboard: %s' % index)
            
            row = df_DB.iloc[0].values.tolist() # convert row to list
            
            #print('\nlen(row): %s' %len(row))
            time.sleep(1)
            ws_db.insert_row(row, index) # write list to worksheet
            #print('Written To Dashboard: %s' % index)
       
        else:
             # Convert data frame to a list
            df = df_dashboard.astype(str)
            df = df.replace('nan','',regex=True)
            row = df_dashboard.iloc[0].values.tolist() # convert row to list
            print('Writing To Dashboard: %s' % index)
            time.sleep(1)
            ws_db.insert_row(list(df_dashboard), 1)
            ws_db.insert_row(row, 2) # write list to worksheet
            print('Written To Dashboard: %s' % index)

            dup_state = False
            
        
    
    except Exception as err:
        if str(err)!= ws_title:
            slack_post(err_chnl, str(err))
            print('Formdef Exception: ',err)
        
        if df_dashboard is not None:
            df = df_dashboard.astype(str)
            df = df.replace('nan','',regex=True)
            index = len(df)
            try:
                ws_db = gsheet.add_worksheet(title = ws_title, rows = str(10000), cols = len(list(df))) # create worksheet
                # Convert data frame to a list
                row = df.iloc[0].values.tolist() # convert row to list
                print('Writing To Dashboard: %s' % index)
                time.sleep(1)
                ws_db.insert_row(list(df), 1)
                ws_db.insert_row(row, 2) # write list to worksheet
                print('Written To Dashboard: %s' % index)
                
            except Exception as err:
                print(err)
            
        
        dup_state = False
    
    return dup_state
    

def json_tracker(dir_x):
    Dir = dir_x.replace('/qctrack.json','')
    print(Dir)
    if not os.path.exists(Dir):
        os.makedirs(Dir)
    
    f = open(dir_x,"w+")
    json.dump({'StartDate': '','CompletionDate':'','KEY':'', 'ExportTime':'','failedRecharges': [], 'finalTrial':[]}, f) # write to json file
    f.close()
            
    # read the track file
    return read_json_file(dir_x)

def in_preload(df_survey,df_preload,pre_identifier = 'pre_id_learner', identifier = 'id_learner', id_type = int):

    
    for i in df_survey.index.values:
        
        if not pd.isnull(df_survey.at[i,identifier]):
            df_pre_row = df_preload[df_preload[pre_identifier] == int(float(df_survey.at[i,identifier]))]
        else:
            df_pre_row = []
        
        if len(df_pre_row) >0:
            idx = df_pre_row.index.values[0]
            for col in df_pre_row.columns:
                #print('col: %s, pre_val: %s' %(col,df_pre_row.at[idx,col]))
                df_survey.at[i,col] = str(df_pre_row.at[idx,col])
            
            df_survey.at[i,'pre_match'] = 1
        else:
            df_survey.at[i,'pre_match'] = 0
       
    return df_survey

# pre process dataframe
def pre_process(df_survey, grade):
    # rename and format column
    df_survey = df_survey.rename(columns = {'end_time':'CompletionDate'})
    df_survey.columns = df_survey.columns.str.replace("[-]", "_")
    
    df_survey = df_survey.sort_values(by = 'CompletionDate')
    
    # format values in specific columns
    for i in df_survey.index.values:
        
        starttime = df_survey.at[i,'start_time']
        endtime = df_survey.at[i,'CompletionDate']

        df_survey.at[i,'grade'] = grade # assign grade
        df_survey.at[i,'grade'] = grade # assign KEY
        
        if not pd.isnull(starttime):
            df_survey.at[i,'start_time'] = date_time(format_date_time(str(datetime.fromtimestamp(int(starttime)/1000)),'%b %d, %Y   %H:%M:%S'))
        else:
            df_survey.at[i,'start_time'] = date_time(format_date_time(str(datetime.fromtimestamp(int(endtime)/1000)),'%b %d, %Y   %H:%M:%S'))
        
        if not pd.isnull(endtime):
            df_survey.at[i,'CompletionDate'] = date_time(format_date_time(str(datetime.fromtimestamp(int(endtime)/1000)),'%b %d, %Y   %H:%M:%S'))
        else:
            df_survey.at[i,'CompletionDate']  = date_time(format_date_time(str(datetime.fromtimestamp(int(starttime)/1000)),'%b %d, %Y   %H:%M:%S'))
            
        print('start_time: %s | endtime %s' %(df_survey.at[i,'start_time'],df_survey.at[i,'CompletionDate']))
    
    return df_survey


def in_df(df,value = None, key = None,head = 'Duplicate', slack_channel = 'pydata_err', gsheet_url=None):
    
    if key is not None:
        try:
            if str(value) != 'nan':
                
                key =  str(key).replace("'","").strip()
                df[key] = df[key].astype(str)
                
                df_recs = df[df[key] == str(value)]
                
                if len(df_recs)>0:
                    return True
                else:
                    return False
            
        except Exception as err:
            err = str(err).replace('col{','${')
            err_msg = '*%s:* \n`KeyError:`  The variable %s is not in the dataset. Therefore, it could no be used to determine the duplicate.\n%s' %(head,err,gsheet_url)
            slack_post(slack_channel, err_msg)
    return False


# change column

def str_to_datetime(df):
    
    for i in df['CompletionDate'].index.values:
        date_before = str(df.at[i,'CompletionDate'])
        
        print(date_before)
        
        if str(df.at[i,'CompletionDate']) == 'nan':
            df.at[i,'CompletionDate'] ==  df.at[i,'start_time']
        else:
            
            print('S2D: ',date_before)
            if '[' in date_before and ']' in date_before:
                dt_list = date_before.split("'")
                df.at[i,'CompletionDate'] = date_time(dt_list[-2])
            
            else:
                df.at[i,'CompletionDate'] = date_time(date_before)
            
        date_after = df.at[i,'CompletionDate']
        
        #print('Before: %s (%s) | After: %s (%s)' %(date_before, type(date_before), date_after,type(date_after )))
    return df


def col_to_int(df, col):
    for i in df.index.values:
        
        print(df.at[i,col])
        
        if not pd.isnull(df.at[i,col])  :
            df.at[i,col] = int(float(df.at[i,col]))
            print(df.at[i,col] )
            
    return df


def add_key(df):
    for i in df.index.values:
        df.at[i,'KEY'] = str(uuid()) # assign key to submission
    
    return df



# upload file to s3 file storage.
def s3upload(file_local, bucket_name, file_remote):
    json_s3 = read_json_file('./data/authentication/aws/s3_access.json')
    ACCESS_ID = json_s3['ACCESS_ID']
    ACCESS_KEY = json_s3['ACCESS_KEY']
    s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key= ACCESS_KEY)
    s3.meta.client.upload_file(file_local, bucket_name, file_remote,ExtraArgs={'ACL':'public-read'})
    
    
#***************** CORRECTION SHEETS FUNCTIONS *****************#
#***************************************************************#
# filter data by condition or relavance
def filter_by_relevance(relevance, df_survey):
    
    rel_split = str(relevance).split(' and ')
    
    for j in range(len(rel_split)):
        condition = str(rel_split[j].replace('==','=')).split('=')
            
        # get the variable name and value
        col =  str(condition[0]).strip()
        value = str(condition[1]).replace('"','').strip()
        
        if value.isdigit():
            value = int(value)
        
        try:
            colx = str(get_substring('{', '}',col)).strip()
        except:
            colx = str(col.replace("'",'')).strip()
            
        #print('col : %s val: %s valType: %s'%(colx,value,type(value)))
            
        df_survey = df_survey[df_survey[colx]== value]
        
    return df_survey

# perform corrections on the observations
def correct_obs(correction, df_survey, index):
    cor_split = correction.split(' and ')
    
    for j in range(len(cor_split)):
        condition = cor_split[j].split('=')
            
        # get the variable name and value
        col =  str(condition[0]).strip()
        
        value = str(condition[1]).replace('"','').strip()
        if is_number(value):
            value = float(value)

        colx = str(get_substring('{', '}',col)).strip()
        df_survey.at[index,colx] = value
        
    return df_survey

# delete or drop observations based on relevance
def drop_by_relevance(relevance, df_survey):
    # filter observations by relevance
    df_filt = filter_by_relevance(relevance, df_survey)

    for k in df_filt.index.values:

        df_survey = df_survey.drop(k)

    return df_survey

# drop observations or replace variable values in observations
def gsheet_corrections(action, relevance, corrections, df_survey):
    # Drop Observation
    if str(action).strip() == 'drop':

        df_survey = drop_by_relevance(relevance, df_survey)

    # Replace values in Observation
    elif str(action).strip() == 'replace':

        df_filt = filter_by_relevance(relevance, df_survey)
        
        for k in df_filt.index.values:
            cor_split = corrections.split(' and ')
            
            for j in range(len(cor_split)):
                condition = cor_split[j].split('=')

                # get the variable name and value
                col =  str(condition[0]).strip()

                value = str(condition[1]).replace('"','').strip()
                if is_number(value):
                    value = float(value)

                colx = str(get_substring('{', '}',col)).strip()
                df_survey.at[k, colx] = value
    
        
    return df_survey

# perform corrections in the given data set
def data_corrections(df_corrections, df_survey):
    
    df_surv_c = deepcopy(df_survey)
    
    for i in df_corrections.index.values:
        
        field = df_corrections.at[i,'field']
        relevance = df_corrections.at[i,'relevance']
        correction = df_corrections.at[i,'new_value']

        # perform corrections
        df_surv_c = field_corrections(df_data = df_surv_c, field = field, new_value = correction, relevance = relevance)
    
    return df_surv_c


# get the json queueing file
def json_queue_file(directory = './data/project-queue' , filename = 'async_queue.json'):
    filepath = '%s/%s'%(directory,filename) # filepath to queueing file

    # Create directory and queueing file
    if not os.path.exists(directory):
        os.makedirs(directory) # create queueing directory
        # create queueing file
        dct_q  = {'prev_proj_start_time':'', 'ahead_proj_start_time':''}
        
        file = open(filepath,'w+')
        json.dump(dct_q,file)
        file.close()

    # read file from filepath    

    with open(filepath) as file:
        json_q = json.load(file)
    print( 'Queueing file location: ', filepath )

    return json_q

# get the waiting time for the current project
def proj_wait_time(tmax = 200, directory = './data/project-queue' , filename = 'async_queue.json' ):
    req_time = now()
    # read json file
    json_q = json_queue_file(directory = './data/project-queue' , filename = 'async_queue.json')
    prev_time  = json_q['prev_proj_start_time']
    ahead_time = json_q['ahead_proj_start_time']
    
    filepath = '%s/%s' %(directory,filename)
    # update variables
    if prev_time == '':
        json_q['prev_proj_start_time'] = str(req_time)
        json_q = write_to_json(filepath, json_q)
        wait_time = 0
        #print('waitTime:  %s'%(wait_time))
    else:
        # queueing variables
        start_time = date_time(prev_time)
        ahead_time = date_time(ahead_time)
        #projects = df_q['queued_projects']

        if req_time >= start_time:
            
            td = req_time - start_time
            x = time.strptime(str(td).split('.')[0],'%H:%M:%S')
            secx = timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
            
            print(secx)
            if secx <= tmax:
                secX = int(tmax - secx)
            else:
                secX = 0
            
                
            t0 = start_time - ahead_time
            y = time.strptime(str(t0).split('.')[0],'%H:%M:%S')
            secy = timedelta(hours=y.tm_hour,minutes=y.tm_min,seconds=y.tm_sec).total_seconds()
            print(secy)
            if secy <= tmax:
                secY = int(tmax - secy)
            else:
                secY = 0   
            
            wait_time = secY + secX
            #print('waitTime: %s + %s =  %s'%(secX,secY,wait_time))
            
                  
        else:
            wait_time = tmax
    
    
    json_q['ahead_proj_start_time'] = json_q['prev_proj_start_time'] 
    json_q['prev_proj_start_time'] = str(req_time)
    json_q = write_to_json(filepath, json_q)
    #print(json_q)
    

    return wait_time


def save_corrections(gsheet, filepath, form_id):
    try:
        print('\nCorr 1')
        filename_2 = '%s(corrections).csv' % form_id
        ws_corrections = gsheet.worksheet('corrections')
        print('\nCorr 2')
        df_corrections = df_worksheet(ws_corrections)
        print('\nCorr 3')
        
        df_cor = df_corrections.dropna(how = 'all')
        print('\nCorr 4')
        
        if not df_cor.empty:
            df_data = pd.read_csv(filepath) # read raw dataset
            print('\nPERFORM CORRECTIONS')
            try:
                df_surv_c = data_corrections(df_cor, df_data) # corrected version
            except Exception as err:
                print('\nCorrection Excpetion: %s' %err)
            print('\nPERFORMED CORRECTIONS')
            # write to csv file
            filepath_2 = './data/projects/%s/%s'%(form_id,filename_2)
            df_str_c = df_surv_c.astype(str).replace('nan','',regex=True)
            df_str_c.to_csv(filepath_2, index= False)
            print('\nCOMPLETED CORRECTIONS')
        else:
            print('The Corrections Sheet Is Empty.')

    except Exception as err:
        print('There are no corrections')
    
def field_corrections(df_data, field, new_value, relevance):
    df_row = filter_by_relevance(relevance, df_data) # filter dataset by relevance
    # make corrections
    for i in df_row.index.values:
        df_data.at[i,field] = new_value
    return df_data
        
