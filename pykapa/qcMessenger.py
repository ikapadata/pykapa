from pykapa.quality_functions import surveyCTO_download, dct_xls_data, qc_fields, qc_messenger, slack_post, save_corrections, open_google_sheet
from pykapa.gen_funcs import user_inputs, read_json_file, local_csv
from pykapa.drop_box import to_dropbox
from pykapa.xls_functions import date_time, now
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
err = inputs['ERROR']

#--------------------------------------------------------------------------------------------------------------------
#    2.                                              Quality Control
#--------------------------------------------------------------------------------------------------------------------

if google_sheet_url != ''  and  email != '' and  password != '' and  server != '':
    
    if err != '':
        err = '`ConnectionError:`\n' + err
        slack_post(err_chnl,err)
        print(err)
    
    else:
        
        print('\npykapa has started tracking your data and will send alerts to your specified messenger app.')
        
        while True:
            
            dct_xls = dct_xls_data(google_sheet_url, err_chnl)  # process google sheet and return dictionary
            
            if "error" in list(dct_xls):
                err_msg= dct_xls['message']
                
                if "error" in err_msg:
                    dct_xls = None  
                else:
                     break

            else:
                
                try:
                    if dct_xls is not None:
                            
                        form_id = dct_xls['form_id']
                        df_msg = dct_xls['messages']
                        # Creat local filepath and write data to csv
                        filename = '%s.csv' % form_id
                        filepath = './data/projects/%s/%s' % (form_id, filename)
                           
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
                
                                
                                local_csv(filepath, df_survey) #write to csv
            
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
                        gsheet = open_google_sheet(google_sheet_url)
                        save_corrections(gsheet, filepath, form_id) # perform corrections and save file
                        to_dropbox(gsheet,form_id) # upload files local datasets to dropbox
            
                        
                except Exception as err:
                    err_msg = '`Warning:` \n%s. \n\nYou may have to follow up on this. The backend is still running' % err
                    print(err_msg)
                    slack_post(err_chnl,err_msg)
                  
          
            print('The End %s' % now())
            time.sleep(600)