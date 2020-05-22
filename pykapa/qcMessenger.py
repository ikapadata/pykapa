from quality_functions import surveyCTO_download, dct_xls_data, qc_fields, qc_messenger, slack_post, save_corrections, open_google_sheet, timezone_sast
from gen_funcs import user_inputs, read_json_file, local_csv,write_to_json
from drop_box import *
from xls_functions import date_time, now
import pandas as pd
import time
import os

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
print(email)
print(password)
print(server)
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
            
            qFile = './data/queue/qFile.json'
            
            if  os.path.isfile(qFile):
               try:
                   json_qFile = read_json_file(qFile)
               except Exception as err:
                   print('Queueing Exception: %s'%err)
                   json_qFile ={'starttime': now()}
                   
               starttime = date_time(json_qFile['starttime'])
               
               dt = ( now() - starttime ).total_seconds()
               
               if dt < 100:
                   waittime = 100 - dt
                   print('\nWaiting %s to minimze reaching Google Sheets quota limit.'%(waittime))
                   time.sleep(waittime)
                   
               
            starttime = str(now()) #{'starttime':str(now())}
            print('#STARTTIME: %s'%starttime)
            dct_time = {'starttime':starttime}
            
            try:
                write_to_json(qFile,dct_time)
            except Exception as err:
                print('Queueing Exception: %s' %err)

            
            dct_xlsform = dct_xls_data(google_sheet_url, err_chnl)  # process google sheet and return dictionary
        
            if type(dct_xlsform) == str:
                
                dct_xlsform = None  
              
            else:
                
                try:
                    if dct_xlsform is not None:
                            
                        form_id = dct_xlsform['form_id']
                        df_msg = dct_xlsform['messages']
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
                
                                
                                df_data = local_csv(filepath, df_survey) #write to csv
            
                                # extract columns relevant to qc messages
                                cols = qc_fields(dct_xlsform)
                                svy_cols = list(set(list(df_survey)).intersection(set(cols)))
                            
                                try:
                                    df_surv = df_survey[svy_cols]
                                except Exception as err:
                                    slack_msg = '`Google Sheet Error:(Incorrect Variable)`\n'+ str(err)
                                    print(slack_msg)
                                    slack_post(err_chnl, slack_msg)
                                    break
                                    
                                # perform quality control and post messages on slack
                                print('\n ***** Preparing to ')
                                qc_messenger(df_surv, dct_xlsform, qc_track, err_chnl, google_sheet_url ) # perform quality control, post messages, and send incentives   
                            
                            else:
                                print('No new data.')
                                df_surv = pd.DataFrame()
                        else:
                            print('surveyCTO data set is unrecognized.')
                            
                            df_surv = pd.DataFrame()
                            
                            break
                            
                        # Perform Corrections and backup file on Dropbox
                        gsheet = open_google_sheet(google_sheet_url)
                        
                        save_corrections(gsheet, filepath, form_id) # perform corrections and save file
                        #to_dropbox(gsheet,form_id) # upload files local datasets to dropbox
                        
                        try:
                            df_xpt = dct_xlsform['export']
                            # read survey worksheet
                            df_svy = dct_xlsform['select']
                        except Exception as err:
                            print('\nExport Worksheet Exception: %s' %err)
                        
                        
                        
                        df_data = pd.read_csv(filepath)
                        
                        
                        # EXPORT TO DROPBOX
                        try:
                            print('\n# EXPORT TO DROPBOX')
                            db_xpt_args = export_parameters(df_xpt,export_type = 'dropbox')
                            #df_data_xpt = export_data(df_vst, df_svy, export_type = 'dropbox', relevance = db_xpt_args['relevance'], export_field = 'pykapa_export', select_fields = db_xpt_args['fields'])
                            to_dropbox(apikey = db_xpt_args['api_key'], path = db_xpt_args['path'], form_id = form_id, total_recs = len(df_data), recs = len(df_surv), err_chnl = err_chnl)  
                        except Exception as err:
                            print('\nDropbox Export Exception: %s' %err)
                        
                        # EXPORT TO AIRTABLE
                        try:
                            print('\n# EXPORT TO AIRTABLE')
                            xpt_args = export_parameters(df_xpt,export_type = 'airtable')
                            df_data_xpt = export_data(df_data, df_svy, export_type = 'airtable', relevance = xpt_args['relevance'], export_field = 'pykapa_export', select_fields = xpt_args['fields'])
                            to_airtable(df_data_xpt, table_name =xpt_args['name'], apikey = xpt_args['api_key'], base_key = xpt_args['path'] ,view=xpt_args['view'])
                        except Exception as err:
                            print('\nAirtable Export Exception: %s'%err)
                            
                        # EXPORT TO FORMDEF  
                        try:
                            print('\n# EXPORT TO FORMDEF')
                            xpt_args = export_parameters(df_xpt,export_type = 'formdef')
                            df_formdef = export_data(df_data, df_svy, export_type = 'formdef', relevance = xpt_args['relevance'], export_field = 'pykapa_export', select_fields = xpt_args['fields'])
                            
                            worksheet = xpt_args['name']
                            
                            try:
                                ws_db = gsheet.worksheet(worksheet)
                                df2 = get_as_dataframe(ws_db)
                                cols = list(df2)
                                
                                
                                df_data_xpt = df_formdef[cols]
                            except Exception as err:
                                print("Formdef Export Exception: Sheet Doesn't Exist [%s]"%err)
                                ws_db = gsheet.add_worksheet(title = worksheet, rows = str(10000), cols = len(list(df_data_xpt))) # create worksheet
                        
                            set_with_dataframe(ws_db, df_formdef)
                            
                        except Exception as err:
                            print("Formdef Export Exception: %s"%err)
                            
            
                            
                except Exception as err:
                    err_msg = '`Warning:` \n%s. \n\nYou may have to follow up on this. The backend is still running' % err
                    print(err_msg)
                    slack_post(err_chnl,err_msg)
                  
          
            print('The End %s' %timezone_sast( str( now() )) )
            time.sleep(600)