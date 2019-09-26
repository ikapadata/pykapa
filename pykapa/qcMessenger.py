from quality_functions import *
from incentives_functions import *
import re
import xls_functions
import validators
#--------------------------------------------------------------------------------------------------------------------
#                                                  User Inputs
#--------------------------------------------------------------------------------------------------------------------
# google sheet url
print('Enter the link to your Google Sheet.')
google_sheet_url = input('link: ')
print('\nEnter SurveyCTO server credentials.')
email = input('Email: ')
password = input('Password: ')
server = input('server: ')
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
        err = err + '*GoogleSheet: * the link to your google sheet is invalid. Please enter a valid link to continue.\n'
        
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
            
            df_xls = df_xls_data(google_sheet_url, err_chnl)  # retrieve quality control and incentive data from xls form as a dictionary
            if df_xls is not None:
                print(True)
                df_survey = surveyCTO_download(server,email,password,df_xls['form_id'],err_chnl) # retrieve data from surveyCTO as dataframe
                 
                if df_survey is not None:         
                    if df_survey.empty == False and list(df_survey)!= ['error']:
                        df_surv = reduce_cols_in_surveyData(df_survey,df_xls) # only get cols relevant to messages, incentives, etc.
                        qc_messenger(df_surv,df_xls,err_chnl,google_sheet_url ) # perform quality control, post messages, and send incentives          

                else:
                    break
            else:
                break
        
              
            import time
            print('The End')
            time.sleep(600)