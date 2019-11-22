from quality_functions import *
from incentives_functions import *
import validators
from gen_funcs import user_inputs
from drop_box import dbx_upload

import os

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
            
            dct_xls = df_xls_data(google_sheet_url, err_chnl)  # retrieve quality control and incentive data from xls form as a dictionary
            
            if dct_xls is not None:
                
                form_id = dct_xls['form_id']
                df_msg = dct_xls['messages']


                df_survey = surveyCTO_download(server,email,password,form_id,err_chnl) # retrieve data from surveyCTO as dataframe

                if df_survey is not None: 
                    
                    if not df_survey.empty  and list(df_survey)!= ['error']:
                        # read json tracker
                        print('len(df_survey): %s'%len(df_survey))
                        dir_x = './data/%s/qctrack.json' % form_id
                        qc_track = read_json_file(dir_x)
                        
                        if qc_track['CompletionDate'] != '':
                            if type(qc_track['CompletionDate']) == str:
                                df_survey['CompletionDate'] = pd.to_datetime(df_survey['CompletionDate'])
                            
                            date_old = date_time(qc_track['CompletionDate']) # date from the JSON file that stores the last record
                            df_survey =  df_survey[ df_survey.CompletionDate > date_old ] 
    
                        
                        # replace hyphens with underscores in column names
                        df_survey.columns = df_survey.columns.str.replace("[-]", "_")
                        
                        # filename for raw data
                        filename = '%s.csv' % form_id
                        filepath = './data/%s/%s' % (form_id, filename)

                        #write to csv
                        if os.path.isfile(filepath):
                            df_0 = pd.read_csv(filepath)
                            # append unique data
                            df_ = pd.concat([df_0, df_survey]).drop_duplicates(subset = ['KEY'], keep = 'first')                              
                            df_.to_csv(filepath, index= False)
                        else:
                            df_survey.to_csv(filepath, index= False)
                              
                
                        # reduce columns 
                        cols = qc_fields(dct_xls)
                        svy_cols = list(set(list(df_survey)).intersection(set(cols)))
                        
                        try:
                            df_surv = df_survey[svy_cols]
                        except Exception as err:
                            slack_msg = '`Google Sheet Error:(Incorrect Variable)`\n'+ str(err)
                            print(slack_msg)
                            slack_post(err_chnl, slack_msg)
                            break
                        
                        qc_messenger(df_surv, dct_xls, qc_track, err_chnl, google_sheet_url ) # perform quality control, post messages, and send incentives   
                        
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
                        df_surv_c = data_corrections(df_cor, df_data) # corrected version
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