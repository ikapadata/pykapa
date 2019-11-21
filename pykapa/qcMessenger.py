from quality_functions import *
from incentives_functions import *
import re
import xls_functions
import validators

from drop_box import dbx_upload

import os

import time

#--------------------------------------------------------------------------------------------------------------------
#                                                  User Inputs
#--------------------------------------------------------------------------------------------------------------------
# google sheet url
print('Enter the link to your Google Sheet.')
google_sheet_url = input('link: ')

# surveycto credentials
if os.path.isfile('./data/authentication/surveycto/scto_credentials.json'):
    json_scto = read_json_file('./data/authentication/surveycto/scto_credentials.json')
    email = json_scto['EMAIL']
    password = json_scto['PASSWORD']
    server = json_scto['SERVER']
else:
    print('\nEnter SurveyCTO server credentials.') 
    email = input('Email: ')
    password = input('Password: ')
    server = input('server: ')
    
    # store surveycto credentials and bot_token information
    sctoCred = {"EMAIL":email,"PASSWORD":password,"SERVER":server}
    write_to_json(filepath= './data/authentication/surveycto/scto_credentials.json', data = sctoCred)

# Slack Information
slk_file = './data/authentication/slack/bot_token.json'
if not os.path.isfile(slk_file):
    print('\nEnter Slack Bot Token' )
    bot_token = input('Bot Token: ')
    
    write_to_json(filepath= slk_file, data = {'BOT_TOKEN':bot_token})

    
    
print('\nEnter the channel name to post python errors.')
err_chnl = input('Channel Name: ').lower()

#--------------------------------------------------------------------------------------------------------------------
'''The following lines retrieve data from surveyCTO and Google sheets. 
Then perform a quality check on the data from surveyCTO and post relevant quality 
control issues on ikapadata.slack.com'''



if google_sheet_url != ''  and  email != '' and  password != '' and  server != '':
    
    err = ''
    # add correct prefix to validate links
    if 'http' not in google_sheet_url and 'docs.google' in google_sheet_url.lower():
        google_sheet_url = 'https://' + google_sheet_url
 
    if 'http' not in server and 'surveycto.com'in server.lower():
        server = 'https://' + server

    # validate the link to google sheet
    try:
        google_resp = requests.get(google_sheet_url)
        
    except:
        err = err + '*GoogleSheet:* the link to your google sheet is invalid. Please enter a valid link to continue.\n'
        
    # validate the link to  server
    try:
        server_resp = requests.get(server)
    except:
        err = err + '*SurveyCTO:* the server name or credentials are incorrect. Please enter correct details to continue.\n'
    
    # validate email input
    if not validators.email(email):
        err = err + '*InvalidEmail:*  your email ('+email+') is invalid. Please enter a valid email to continue.\n'   
 
    if err != '':
        err = '`ConnectionError:`\n' + err
        slack_post(str(err_chnl),err)
        print(err)
    
    else:
        
        print('\npykapa has started tracking your data and will send alerts to your specified messenger app.')
        while True:
            
            '''
            dir_queue = './data/async_queue.json'
            
            # ASYNC QUEUEING T0 MINIMIZE SURVEYCTO 409 AND GOOGLE SHEETS API 429 ERRORS
            
            #wait_time = proj_wait_time(tmax = 200, directory = './data/project-queue' , filename = 'async_queue.json' )
            
           #time.sleep(wait_time)
           '''
            
            df_xls = df_xls_data(google_sheet_url, err_chnl)  # retrieve quality control and incentive data from xls form as a dictionary
            
            if df_xls is not None:
                
                form_id = df_xls['form_id']
                
                df_msg = df_xls['messages']


                df_survey = surveyCTO_download(server,email,password,form_id,err_chnl) # retrieve data from surveyCTO as dataframe

                if df_survey is not None: 
                    
                    if not df_survey.empty  and list(df_survey)!= ['error']:
                        # read json tracker
                        print('len(df_survey): %s'%len(df_survey))
                        dir_x = make_relative_dir('data', form_id, 'qctrack.json')
                        qc_track = read_json_file(dir_x)
                        
                        if qc_track['CompletionDate'] != '':
                            if type(qc_track['CompletionDate']) == str:
                                df_survey['CompletionDate'] = pd.to_datetime(df_survey['CompletionDate'])
                            
                            date_old = date_time(qc_track['CompletionDate']) # date from the JSON file that stores the last record
                            df_survey =  df_survey[ df_survey.CompletionDate > date_old ] 
    
                        
                        # replace hyphens with underscores in column names
                        df_survey.columns = df_survey.columns.str.replace("[-]", "_")
                        # filename for downloaded data
                        filename = '%s.csv' % form_id
                        filepath = make_relative_dir('data', form_id, filename)
                        #write to csv
                        if os.path.isfile(filepath):
                            df_0 = pd.read_csv(filepath)
                            # append unique data
                            df_ = pd.concat([df_0, df_survey]).drop_duplicates(subset = ['KEY'], keep = 'first')  
                            
                            df_.to_csv(filepath, index= False)
                        else:
                            df_survey.to_csv(filepath, index= False)
                              
                
                        # reduce columns 
                        cols = qc_fields(df_xls)
                        svy_cols = list(set(list(df_survey)).intersection(set(cols)))
                        
                        try:
                            df_surv = df_survey[svy_cols]
                        except Exception as err:
                            slack_msg = '`Google Sheet Error:(Incorrect Variable)`\n'+ str(err)
                            print(slack_msg)
                            slack_post(err_chnl, slack_msg)
                            break
                        
                        qc_messenger(df_surv, df_xls, qc_track, err_chnl, google_sheet_url ) # perform quality control, post messages, and send incentives   
                        
                        # perform corrections:

                else:
                    print('surveyCTO data set is unrecognized.')
                    break
                
                gsheet = open_google_sheet(google_sheet_url)
                
                try:
                    filename_2 = '%s(cor).csv' % form_id
                    ws_corrections = gsheet.worksheet('corrections')
                    df_corrections = df_worksheet(ws_corrections)
                            
                    df_cor = df_corrections.dropna(how = 'all')
                            
                    if not df_cor.empty:
                        df_data = pd.read_csv(filepath)
                        df_surv_c = data_corrections(df_cor, df_data)
                        # write to csv file
                        filepath_2 = './data/%s/%s'%(form_id,filename_2)
                        df_surv_c.to_csv(filepath_2, index= False)
                    else:
                        print('There are no corrections')
                                
                except Exception as err:
                    print('There are no corrections')
                    
                try:
                    ws_dropbox = gsheet.worksheet('to_dropbox')
                    df_dropbox = df_worksheet(ws_dropbox)
                    
                    dir_lcl = './data/%s'% form_id
                    dbx_upload( dir_local = dir_lcl, dir_dropbox = df_dropbox.at[0,'dropbox_dir'], dbx_token = df_dropbox.at[0,'dropbox_token'])
                    
                except Exception as err:
                    print(err)
                    print("`DropBox BackUps`.\nAdd the _to_dropbox_ worksheet and specify the DropBox directory and token to enable DropBox Backups.")
                
            else:
                print('Google Sheet unrecognized.')
                break
        
              
            import time
            print('The End %s' % now())
            time.sleep(600)