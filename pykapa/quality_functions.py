import json
import gspread
import os
import os.path
import json
import requests
import pytz
import locale
import numpy as np
import pandas as pd
import re
import unicodedata

from requests.auth import HTTPDigestAuth
from xls_functions import *
from incentives_functions import *
from slackclient import SlackClient
from oauth2client import file, client, tools
from locale import atof
from gspread_dataframe import *
from pprint import pprint
from googleapiclient import discovery


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
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('date-time','date_time')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace('format-date-time','format_date_time')
                dataframe.loc[i,element] = dataframe.loc[i,element].replace(dataframe.loc[i,element], format_funcstr(dataframe.loc[i,element], 'jr:choice-name'))
                
    
    return dataframe


# retrieve relevant data from google sheet
def df_xls_data(google_sheet_url, err_chnl = None):
    # 1. open worksheets in google sheet
    try:
        google_sheet = open_google_sheet(google_sheet_url)              # Open google sheet
        ws_svy =  google_sheet.worksheet('survey')                      # Open the worksheet named survey
        ws_choices =  google_sheet.worksheet('choices')                 # Open the worksheet named choices
        ws_set = google_sheet.worksheet('settings')                     # Open the worksheet named settings
    except Exception as error:
        error =  '`ConnectionError:`\n*GoogleSheet: * the link is invalid or missing worksheets (survey, choices, settings). Please correct the mistake to continue.\n'
        slack_post(str(err_chnl),error) # send message to slack
        return None
    
    # a. Read optional worksheets    
    if str(google_sheet) != str(None):
    
        # a(i) read messages and messages_settings worksheets
        try:
            ws_msg = google_sheet.worksheet('messages')                     # Open the worksheet named messages
            ws_msgset = google_sheet.worksheet('messages_settings')         # Open the worksheet named messages_settings
        except:
            err = '`Google Sheet Error: (Missing Worksheets)` \nMissing *messages* or *messages_settings* worksheet(s), check and add the missing worksheet(s) at the link below to continue. \n'+ google_sheet_url
            slack_post(err_chnl,err)
            return None

        # a(ii) read incentives_settings worksheet   
        try:
            ws_incentives =  google_sheet.worksheet('incentives_settings')  # Open the worksheet named incentives_settings
        except:
            ws_incentives = None   
    
        # 2. convert worksheets into DataFrames
        df_svy = df_worksheet(ws_svy).replace('', np.NaN).dropna(how='all') # convert ws_svy to DataFrame
        df_svy = df_svy.replace(np.NaN, str(np.NaN))
        
        df_choices = df_worksheet(ws_choices).replace('', np.NaN).dropna(how='all')# convert ws_choices to DataFrame
        df_choices = df_choices.replace(np.NaN, str(np.NaN))
        
        df_set_xls = df_worksheet(ws_set).replace('', np.NaN).dropna(how='all') # convert ws_set to DataFrame
        df_set_xls = df_set_xls.replace(np.NaN, str(np.NaN))
        
        # a. convert optional worksheets into DataFrames
        # a(i) convert messages and messages_settings worksheets into DataFrames
        if str(ws_msg) != str(None) and str(ws_msgset) != str(None):
            df_msg_xls = df_worksheet(ws_msg)
            msg = df_msg_xls
            df_msgset_xls = df_worksheet(ws_msgset)
            msg_set = df_msgset_xls
         
            if df_msg_xls.empty==False and df_msgset_xls.empty==False:
    
                # replace '' or "" with nan
                for idx in df_msg_xls.index.values:
                    df_msg_xls.loc[idx,'message_relevance'] = df_msg_xls.loc[idx,'message_relevance'].replace("''",str(np.NaN))
                    df_msg_xls.loc[idx,'message_relevance'] = df_msg_xls.loc[idx,'message_relevance'].replace('""',str(np.NaN))
        
                # merge data from two worksheets according to channel_id into one dataframe
                df_msg_xls = xls_merge_ws(df_msg_xls,df_msgset_xls)
                # convert xls format to python syntax
                df_msg = xls2py(df_msg_xls).replace('', np.NaN).dropna(how='all')
                df_msg = df_msg.replace(np.NaN, str(np.NaN))
                
            else:
                
                df_msg = pd.DataFrame()
        else:
            df_msg = None
            
        # a(ii) convert incentives worksheet into DataFrame
        if str(ws_incentives) != str(None):
            df_incentives_xls = df_worksheet(ws_incentives).replace('', np.NaN).dropna(how='all')
            # convert xls syntax to python syntax
            df_incentive = xls2py(df_incentives_xls)
            df_incentive = df_incentive.replace(np.NaN, str(np.NaN))
        else: 
            df_incentive = None
    
      
        # 3. Rename choices header
        old_header = df_choices.columns # list of old headers
        new_header = {}                 # empty dictionary
        for header in old_header:
            new_header[header] = 'choice_'+ header # append 'choice_' to the old header
        df_choices.rename(columns = new_header, inplace=True) # rename headers
    
    
        # 4. split type into two variables (type and list_name)
        #df_svy = df_svy.replace(np.NaN, str(np.NaN)) # convert NaN from integer to string, i.e NaN to nan
        # a. add new column (list_name)
        df_svy['list_name'] = str(np.NaN)
        
        # b. assign type to list_name 
        for i in df_svy.index.values:
            vec = df_svy.loc[i,'type'].split() # form vector of strings
            df_svy.loc[i,'type'] = vec[0] # assign first string in vector to type
            if len(vec)!=1:
                df_svy.loc[i,'list_name'] = vec[1] # assign second string to list_name
                
        #create dataframe with only 3 columns to reference types
        sel_cols = ['type','list_name','name','dashboard_state']
        try:
            df_select = df_svy[sel_cols]
        except:
            df_select = df_svy[sel_cols[0:3]]
            
        # 5. obtain form_id of worksheet
        form_id = df_set_xls.loc[0,'form_id']
        
        
        # create dashboard header
        try:
            db_sel  = df_select[df_select.dashboard_state == 'TRUE'] # dashboard dataframe
            db_msg  = df_msg[df_msg.dashboard_state == 'TRUE'] # dashboard dataframe
            db_head =list(dict.fromkeys( list(db_msg['name']) + list(db_sel['name']) ))
            
        except:
            db_head = None
        
        # create dashboard dataframe and create worksheet
        if db_head is not None:
            db = pd.DataFrame(columns = db_head)
            
        else:
            db = None
      
        if err_chnl is not None:
        # Post error messages on slack for missing optional sheets
            if df_msg is not None and df_msg.empty == False:
                #df_msg = df_msg.replace('nan',np.NaN)
                df = df_msg[df_msg.channel_name == 'nan']
                if len(df) != 0:
                    err = '`Google Sheet Error: (Missing Channel Name)` \n'+ str(len(df))+' message(s) have missing channel ID/Name so their alerts will not be posted. Check and add the missing channel ID at the link below to continue. \n'+ google_sheet_url
                    slack_post(err_chnl,err)
                    return None
    
            elif msg.empty == True or msg_set.empty == True:
                err = '`Google Sheet Error: (Empty Worksheets)` \nThe *messages* or *messages_settings* worksheet(s) have no data for alerts, check and add the missing data at the link below to continue. \n'+ google_sheet_url
                slack_post(err_chnl,err)
                return None
            
            elif df_incentive is not None and df_incentive.empty == False:
                # check if there are missing cells and delete the row with any missing cell
                df_incentive = df_incentive.replace('nan',np.NaN).dropna()
                if df_incentive.empty == True:
                    err = '`Google Sheet Error: (No Incentives)` \nThe *incentives_settings* worksheet is missing data in some of its cells, check and add the missing data at the link below to continue.  \n'+ google_sheet_url
                    slack_post(err_chnl,err)
                    return None

    return {'messages': df_msg, 'incentives':df_incentive, 'form_id':form_id, 'select':df_select, 'choices':df_choices,'dashboard':db}

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
        
        if 'CompletionDate' in json_file and json_file['CompletionDate'] != '':
            timestamp = int(date_time(json_file['CompletionDate']).timestamp()*1000)
            # survey url
            svy_url = server + '/api/v1/forms/data/wide/json/'+form_id + '?date='+str(timestamp)
        else:
            svy_url = server + '/api/v1/forms/data/wide/json/'+form_id
            
    else:
        f = open(dirX,"w+")
        json.dump({'StartDate': '','CompletionDate':'','KEY':'', 'ExportTime':'','failedRecharges': [], 'finalTrial':[]}, f) # write to json file
        f.close()
        
        svy_url = server + '/api/v1/forms/data/wide/json/' + form_id
    
    print('surveyCTO link: ', svy_url,)
    #print(dirX)
            
            
    return svy_url


def surveyCTO_response(server,username,password,form_id):
    Dir = make_relative_dir( 'data', form_id,'')
    dirX = concat(Dir, 'qctrack.json')
    print('dirTrack: ',dirX)
    if not os.path.exists(Dir):
        os.makedirs(Dir)
    #create surveyCTO link
    file_url = surveyCTO_link(server,form_id, dirX)
    
    # download json file from surveyCTO
    print('\nRequesting json data from surveyCTO')
    resp = requests.get(file_url, auth=HTTPDigestAuth(username, password))
    
    return resp

# download data from surveyCTO
def surveyCTO_download(server,username,password,form_id, err_chnl = None):
    
    resp = surveyCTO_response(server,username,password,form_id)
    
    status = resp.status_code
    print(status)
    try:
        json_data = resp.json()
        # convert json to dataframe
        df_json = pd.DataFrame(json_data)
    except:
        df_json = pd.DataFrame()
    
    # send messgaes to error channel if download is unsuccessful
    if status == 401:
        err = '`SurveyCTO Error: (' + str(status) + ')`\n' + "The credentials you've entered are invalid. Please enter the correct credentials to continue."
        slack_post(str(err_chnl),err) # post message on slack  
        
        return None
    elif status == 404:
        err = '`SurveyCTO Error: (' + str(status) + ')`\n' + "The server ("+server+" ) you were trying to reach couldn't be found."
        slack_post(str(err_chnl),err) # post message on slack
        return None
    
    elif status != 200 and status != 401 and df_json.empty == False:
        err = '`SurveyCTO Error: (' + str(status) + ')`\n' + df_json.loc['message','error']
        slack_post(str(err_chnl),err) # post message on slack
         
    if len(list(df_json)) > 1:
        # convert string to datetime
        df_json['CompletionDate'] = pd.to_datetime(df_json['CompletionDate'])
        df_json = df_json.sort_values(by=['CompletionDate']) # sort values in ascending order of CompletionDate

    return df_json.replace('', str(np.NaN))


# reduce the dataframe size of df_survey by selecting cols relevant to quality control, incentives, etc.
def reduce_cols_in_surveyData(df_survey,df_xls):
    lst = ['CompletionDate','KEY'] # create list
    for key in df_xls:
        df = df_xls[key]
        print('key: ', key)
        if key == 'messages' or key == 'incentives':
            for item in list(df_survey):
                itm = 'var{'+item+'}'
                if str(df) != str(None) and df.empty == False:
                    for idx in df.index.values:
                        for header in list(df):
                            try:
                                indx = df.loc[idx,header].index(itm)
                                #print('index: ', idx, ' itm: ', itm, 'item: ', item)
                                lst.append(item)
                            except Exception as err:
                                error = err
                                
        elif key == 'dashboard' and str(df) != str(None):
            
            for el in df_survey.columns:
                if el in df.columns:
                    lst.append(el)
  
    # remove duplicates
    lst = list(dict.fromkeys( lst ))
    return df_survey[lst]


#function to post messages to slack
def slack_post(channel_name, slack_msg): 
    
    #Oauth_token = 'xoxp-4366044688-409367540019-432067886036-150195648f340022e8c7f6b0ab073f97'
    bot_token = 'xoxb-4366044688-432067889364-XgC1NpR8FEzwcjB2eoj9rO9U'
    slack_client = SlackClient(bot_token)
    slack_client.api_call('chat.postMessage', channel=channel_name, text=slack_msg)


#--------------------------------------------------------------------------------------------------------------------------------
#                                                      Quality Control functions
#--------------------------------------------------------------------------------------------------------------------------------

# evaluate conditions and post messages to channel    
def control_messenger(df_msg,df_xls,err_chnl=None,df_dashboard=None):
    # list of unique channel names
    df_msg = df_msg.sort_values(by=['channel_id'])
    lst = df_msg.drop_duplicates(subset='channel_name', keep ='first')['channel_name']
    nan = 'nan'    
    for chnl in lst:
        msg = ''
        error = ''
        dfx = df_msg.loc[df_msg['channel_name'] == chnl] # filter by channel name
        for index in dfx.index.values:
            # determine column type
            
            #print('cm_idx: ',index)
            msg_label = evalfunc_str(str(dfx.loc[index,'message_label']).replace('""',''),df_xls)
            #print('row: ', index +2,'msg_rel:', dfx.loc[index,'message_relevance'],' msg_label: ', dfx.loc[index,'message_label'] )
            msg_rel = evalfunc_str(dfx.loc[index,'message_relevance'],df_xls)
            
            header = evalfunc_str(dfx.loc[index,'message_header'],df_xls)
            messenger = dfx.loc[index,'messenger']
            col = dfx.loc[index,'name']

            try:
                if msg_rel != 'nan' and eval(str(msg_rel))==True:
                    msg = msg + msg_label + '\n'
                    
                    value = 1
                    df_dashboard = dashboard(df_dashboard,col,value)

            except Exception as exceptErr:
                index += 2
                msg_rel = msg_rel.replace('var{','${')
                err = '\n`row:` '+str(index) + '\t `message relevance:` '+ str(msg_rel).replace('==','=')+ '\t `error message:` ' + str(exceptErr)
                error = concat(error, err)
        
        chnl = chnl.replace('""','')
        chnl = chnl.replace('"','')
        
        if msg !='' or error !='':
            #print('channel: ',chnl,'\nmsg: \n', msg)
            if messenger.lower() == 'slack':
                slack_msg = header +'\n'+ msg
                slack_msg = slack_msg.replace('""','')
                slack_msg = slack_msg.replace('"','')

                #print('\nMessage channel: ', chnl)
                slack_post(chnl.lower(), slack_msg.replace('nan',''))
                
               
      
            #print('channel: ',chnl,'\nmsg: \n', msg)
            if messenger.lower() == 'slack':
                error = '`Syntax error(s):`\nMake sure xls names ( i.e ${name} ) in the *messages* worksheet exists, spelled correctly, or the same type is compared in the following:\n' + error
                slack_synt = header + '\n' + error
                slack_synt = slack_synt.replace('""','')
                slack_synt = slack_synt.replace('"','')
                   
                chnl = chnl.replace('""','')
                chnl = chnl.replace('"','')
                #print('\nSyntax channel: ', err_chnl)
                slack_post(err_chnl.lower(), slack_synt.replace('nan',''))
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
def qc_messenger(df_survey,df_xls,qc_track, admin_channel = None, google_sheet_url=None):
    
    form_id = df_xls['form_id']
    dirX = make_relative_dir('data', form_id, 'qctrack.json')
    
    if df_survey.empty == False and list(df_survey)!= ['error']:
    
        
        print('\nQC STARTED')
        df_select = df_xls['select']
        df_choices = df_xls['choices']
        
        
        print('recs: ',len(df_survey),'\n')
            
        #Convert number strings with commas in pandas DataFrame to float
        decmark_reg = re.compile('(?<=\d),(?=\d)')
        
        if df_xls['messages'] is not None and df_xls['messages'].empty == False:
        
            for i in df_survey.index.values:
    
                # sort messages in ascending order of channel IDs
                df_msg = df_xls['messages'].sort_values('channel_id',ascending=True)
            
                df_incentive = deepcopy(df_xls['incentives'])
                df_dashboard = deepcopy(df_xls['dashboard'])

                
                for col in df_survey: 
                    # format values from respondent
                    val = df_survey.loc[i,col] # read value from respondent
                    # esnure all values are string
                    if type(val)!= str:
                        val = str(val)

                    value = decmark_reg.sub('.',val) # change decimal point from a comma (,) to a period (.)
                    
                    # determine column type
                    try:
                        colType = df_xls['select'].loc[df_xls['select'][df_xls['select'].name == col].index.values[0],'type']
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
                    df_dashboard = dashboard(df_dashboard,col,value,colType,df_xls)
                    
                    
                    # populate xls variables with data from surveyCTO
                    df_msg = df_msg.replace('var{'+col+'}',str(value), regex = True) # messages
                    df_msg = df_msg.replace('col{'+col+'}',col, regex = True) # messages
                        
                    if df_incentive is not None:
                        df_incentive = df_incentive.replace('var{'+col+'}',str(value), regex = True) # incentives
                        df_incentive = df_incentive.replace('col{'+col+'}',col, regex = True) # incentives


                # evaluate quality control conditions and post messages on channels
                df_db = control_messenger(df_msg, df_xls, admin_channel, df_dashboard)            
                

                # ----------------- send incentives -------------
                if df_incentive is not None and df_incentive.empty == False:

                    for idx in df_incentive.index.values:
                        # recharge details
                        msisdn  = simcontact(evalfunc_str(str(df_incentive.loc[ idx,'contact']),df_xls))
                        #print('xlsContact: ',msisdn, ' simContact: ', simcontact(msisdn) )
                        api_key = evalfunc_str(str(df_incentive.loc[ idx,'flickswitch_api_key']),df_xls)
                        r_count = int(float(evalfunc_str(str(df_incentive.loc[ idx,'recharge_count']), df_xls)))
                        network = evalfunc_str(str(df_incentive.loc[ idx,'network']), df_xls)
                        amount = evalfunc_str(str(df_incentive.loc[ idx,'amount']), df_xls)
                        key = df_survey.loc[i,'KEY']
                        prod_type = evalfunc_str(str(df_incentive.loc[ idx,'incentive_type']), df_xls)
                        
                        if msisdn != 'nan' and network != 'nan':
                            #msisdn = simcontact(msisdn)
                            df_rec = msisdn_history(api_key, msisdn, prodType = prod_type) # check history
                            print('msisdn: ',msisdn, ' hist: ', len(df_rec) )
                            
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
                                    print('Buying',prod_type, 'for msisdn: ', msisdn, '(', network,')')
                                    
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
                              
    
    
                # keep track of the last checked record
                date_new = format_date_time(str(df_survey.loc[i,'CompletionDate']), '%b %d, %Y   %H:%M:%S')
                qc_track['CompletionDate'] = date_new
                qc_track['KEY'] = df_survey.loc[i,'KEY']


                if qc_track['StartDate'] == '' and i == 0:
                    qc_track['StartDate'] = date_new
    
                print('\n',qc_track)
                write_to_json(dirX, qc_track) # record the last checked interview in json fileq               
                to_google_sheet(df_dashboard = df_db, google_sheet_url = google_sheet_url)
    
    return True
                
                
                
def qc_manager(google_sheet_url,username,password,server):
    # create incentive database tables      
    excess_recharge_dir = make_relative_dir('data','db','recharges','excess_recharges.json') #.../data/db/recharges/excess_recharges.json
    create_json_db(excess_recharge_dir)

    recharge_dir = make_relative_dir('data','db','recharges','recharges.json') #.../data/db/recharges/recharges.json
    create_json_db(recharge_dir)

    while True:
        df_xls    = df_xls_data(google_sheet_url)  # retrieve quality control and incentive data from xls form as dataframe
        df_survey = surveyCTO_download(server,username,password,df_xls['form_id']) # retrieve data from surveyCTO json file as dataframe
        qc_messenger(df_survey,df_xls) # perform perform quality control, post messages, and send incentives

        import time
        print('The End')
        time.sleep(60)
        
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

def dataframe_to_worksheet(google_sheet, ws_title, df_data):
    # read worksheet
    ws_dashboard = google_sheet.worksheet(ws_title)
    # convert worksheet to dataframe

        #df_dashboard = df_worksheet(ws_dashboard)
    df_dashboard = get_as_dataframe(ws_dashboard, evaluate_formulas=False)
    
    # write to worksheet 
    len_db = len(df_dashboard)
    print('l_db: ', len_db)

    if len_db == 0:
        set_with_dataframe(ws_dashboard,df_data)
    # append data to existing data
    else:
        try:
            #df_append = df_dashboard.append(df_data,sort=False)
            #print('\ndashboardHeader: \n',df_append.columns)
            df_append = pd.concat([df_dashboard,df_data], sort = False)
            
            time.sleep(1.5) # sleep for 1 sec
            set_with_dataframe(ws_dashboard, df_append, allow_formulas=True)
        except Exception as err:
            print(err)
        
 # dashboard       
def dashboard(df_dashboard,col,value,colType= None,df_xls=None):
    if df_dashboard is not None and col in list(df_dashboard):
        if type(value) == str:
            try:
                value = get_substring('"""', '"""', value)
            except:
                x = None
                
        if str(colType) == 'select_one' and str(value) != 'nan':
            #print('db: ',value)
            if str(value).isdigit():
                value = int(value)
            df_dashboard.loc[0,col] = jr_choice_name(value,col,df_xls)
           # print('value: ', value, ' col: ', col, ' jr: ', jr_choice_name(value,col,df_xls))
            
        elif str(colType) == 'select_multiple':
            lst = []

            for val in value.split(' '):
                
                try:
                    val = int(str(val))
                except Exception as err:
                    err = err
                    
                lst.append(jr_choice_name(val,col,df_xls))
 
            df_dashboard.at[0,col] = str(lst)
            print('new_val: ', lst, 'old_val: ', value, 'col: ', col)
            #lst_vals = value.split(' ')    
        else:
            df_dashboard.loc[0,col] = value

    return df_dashboard


def to_google_sheet(df_dashboard, google_sheet_url, ws_title = 'dashboard'):
    t0 = now()
    # open google sheet and get worksheets
    gsheet = open_google_sheet(google_sheet_url) # open google sheet using url
    wks = gsheet.worksheets() #  list of sheet names
    
    ws_name = "'" + ws_title + "'"
    
    if ws_name not in str(wks):
    # create or read dashboard worksheet
    # create sheet if sheet does not exist, then write to sheet
        ws_db = gsheet.add_worksheet(title = ws_title, rows = str(100000), cols = len(list(df_dashboard))) # create worksheet
        set_with_dataframe(ws_db, df_dashboard)
        return False
    
    # sheet exists so read, then write to sheet
    else:
        # read worksheet
        
        ws_db = gsheet.worksheet(ws_title) # open worksheet 
        # read worksheet in dataframe
        #df_ws = get_as_dataframe(ws_db).dropna(how='all').astype(str)
        recs = ws_db.get_all_records()
        index = len(recs) + 2

        # get headers
        if recs != []:
            ws_head = list(recs[0].keys()) # get headers
        
            #print('db_cols = %s',ws_head)
            
            df_e = pd.DataFrame( columns = list( set(ws_head).difference( list(df_dashboard) ) ) )
            df_DB = pd.concat([df_e,df_dashboard], sort = False).replace(np.nan, '', regex=True)[ws_head]
            #df_DB = pd.concat([df_e,df_dashboard], sort = False).replace('nan', '', regex=True)[ws_head]
            df_DB = df_DB.astype(str)
            
            # Convert data frame to a list
            row = df_DB.iloc[0].values.tolist() # convert row to list
            ws_db.insert_row(row, index) # write list to worksheet
        else:
            set_with_dataframe(ws_db, df_dashboard)
        
        #print('\nELSE: %s' % (now() - t0))
        return True

# write to stata (dta) file
def to_dta(df_json, filepath):

    if os.path.isfile(filepath):
        # read data from the old dta file
        df_dta = pd.read_stata(filepath)
        # append new data to old data
        data = df_dta.append(df_json, sort = False)[list(df_json)]
        # drop duplicates
        data =  data.drop_duplicates(subset = ['KEY'])
    else:
        data = df_json

    # encode data in ascii
    for col in list(data):
        data[col] = data[col].apply(lambda val: unicodedata.normalize('NFKD', str(val)).encode('ascii', 'ignore').decode('utf-8'))

    # write to dta file
    data.to_stata( filepath, encoding = 'ascii', version = 117, write_index = False )

    return data
        
        