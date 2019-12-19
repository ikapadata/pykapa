from quality_functions import surveyCTO_download, dct_xls_data, qc_fields, qc_messenger, slack_post
import validators
from gen_funcs import user_inputs, read_json_file
#from drop_box import to_dropbox
from xls_functions import date_time, now
import requests
import pandas as pd
import time

#--------------------------------------------------------------------------------------------------------------------
#    1.                                              User Inputs
#--------------------------------------------------------------------------------------------------------------------
# take user inputs
inputs = user_inputs()

google_sheet_url  = inputs['SHEET_LINK']
email  = inputs['EMAIL']
password  = inputs['PASSWORD']
server  = inputs['SERVER']
err_chnl  = inputs['ERR_CHNL']

#--------------------------------------------------------------------------------------------------------------------
#    2.                                              Quality Control
#--------------------------------------------------------------------------------------------------------------------

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
        err = err + '*GoogleSheet:* Your google sheet link is invalid. Please enter a valid link to continue.\n'
        
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
            
            try:
                dct_xls = dct_xls_data(google_sheet_url, err_chnl)  # process google sheet and return dictionary
                
                if dct_xls != {}:
                    
                    if dct_xls is not None:
                        
                        form_id = dct_xls['form_id']
                        df_msg = dct_xls['messages']
                       
                        # Download data from surveyCTO
                        df_survey = surveyCTO_download(server,email,password,form_id,err_chnl) # retrieve data from surveyCTO as dataframe
        
                        if df_survey is not None: 
                            
                            if not df_survey.empty  and list(df_survey)!= ['error']:
                                # read json tracker
                                
                                dir_x = './data/projects/%s/qctrack.json' % form_id
                                qc_track = read_json_file(dir_x)
                                
                                # filter data by CompletionDate
                                if qc_track['CompletionDate'] != '':
                                    if type(qc_track['CompletionDate']) == str:
                                        df_survey['CompletionDate'] = pd.to_datetime(df_survey['CompletionDate'])
                                    
                                    date_old = date_time(qc_track['CompletionDate']) # date from the JSON file that stores the last record
                                    df_survey =  df_survey[ df_survey.CompletionDate > date_old ] 
            
                                # Creat local filepath and write data to csv
                                #filename = '%s.csv' % form_id
                                #filepath = './data/%s/%s' % (form_id, filename)
                                #local_csv(filepath, df_survey) #write to csv
        
                                # extract columns relevant to qc messages
                                cols = qc_fields(dct_xls)
                                svy_cols = list(set(list(df_survey)).intersection(set(cols)))
                                
                                try:
                                    df_surv = df_survey[svy_cols]
                                except Exception as err:
                                    slack_msg = '`Google Sheet Error:(Incorrect Variable)`\n'+ str(err)
                                    print(slack_msg)
                                    slack_post(err_chnl, slack_msg)
                                    break
                                
                                # perform quality control and post messages on slack
                                qc_messenger(df_surv, dct_xls, qc_track, err_chnl, google_sheet_url ) # perform quality control, post messages, and send incentives   
                                
                        else:
                            print('surveyCTO data set is unrecognized.')
                            break
                        
                        # Perform Corrections and backup file on Dropbox
                        #gsheet = open_google_sheet(google_sheet_url)
                        #save_corrections(gsheet, filepath, form_id) # perform corrections and save file
                        #to_dropbox(gsheet) # upload files local datasets to dropbox
                    else:
                        print('Google Sheet unrecognized.')
                        break
                    
            except Exception as err:
                err_msg = '`Warning:` \n%s. \n\nYou may have to follow up on this. The backend is still running' % err
                print(err_msg)
                slack_post(err_chnl,err_msg)
              
          
            print('The End %s' % now())
            time.sleep(600)