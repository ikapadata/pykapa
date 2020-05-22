import os
import json
import pandas as pd
import validators
import requests

def user_inputs():
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
    
    
    
    err = ''
    # add correct prefix to validate links
    if 'http' not in google_sheet_url and 'docs.google' in str(google_sheet_url).lower():
        google_sheet_url = 'https://' + google_sheet_url
 
    if 'http' not in server and 'surveycto.com'in str(server).lower():
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
 
    
    return {'SHEET_LINK':google_sheet_url,'EMAIL':email,'PASSWORD':password,'SERVER':server, 'ERR_CHNL':err_chnl, 'ERROR': err}

# read json file
def read_json_file(filepath):
    if os.path.isfile(filepath) == True:
        with open(filepath,'r') as jsonX:
            json_file = json.load(jsonX)
            
        if json_file == '[]':
            json_file = ast.literal_eval(json_file)
        else:
            json_file == json_file
    return json_file    

# write to json file
def write_to_json(filepath, data):
    if os.path.isfile(filepath) == True:
        with open(filepath,'w') as file:
            json.dump(data,file)
    
    else:
        # obtain directory
        filename = filepath.split('/')[-1]
        dir_file = filepath.replace('/%s'%filename,'')
        os.makedirs(dir_file)
        # write to json file
        with open(filepath,'w') as file:
            json.dump(data,file)
       
    # read the file
    json_file =  read_json_file(filepath)           
            
    return json_file

# directory relative to the script
def make_relative_dir(*folders_and_filename):
    import os
    script_path = os.path.abspath('__file__') # path to current script
    script_dir = os.path.split(script_path)[0] #i.e. /path/to/dir/
    abs_file_path = os.path.join(script_dir, *folders_and_filename)
    return abs_file_path

# create an empty json file
def create_empty_json(filepath):
    file = open(filepath, "w") 
    json.dump('[]', file) 
    file.close()

# remove special characters from a string
def remove_specialchar(string):
    return ''.join(e for e in string if e.isalnum())

# substring after the last given char
def substr_after_char(string,char):
    idx = string.rfind(str(char))
    if idx >= 0:
        return string[idx+1:len(string)]
    else:
        return None
    
# create a json database 
def create_json_db(filepath):
    filename = os.path.basename(filepath)
    Dir = os.path.dirname(filepath)
    filelist = os.listdir(Dir)
    
    
    # create empty database table if it doesn't exist
    if filename not in filelist:
        create_empty_json(filepath)

# writing new data to csv file     
def local_csv(filepath, df_survey):
    
    if os.path.isfile(filepath):
       
        df_csv = pd.read_csv(filepath, dtype= str) # read local csv 
        df_surv = df_survey.astype(str).replace('nan','',regex=True) # format survey fields data types to string
        # append new data
        print('\nAPPENDING NEW DATA:')
        df_data = pd.concat([df_csv, df_surv ], sort= False).drop_duplicates(subset = ['KEY'], keep = 'first')#[list(df_survey)]
        print('\nAPPENDED NEW DATA:')
        print("\nDATA\n\n")
        df_data.to_csv(filepath, index= False) # write to csv
    else:
        print('\nWRITING NEW DATA:')
        df_data = df_survey.astype(str).replace('nan','',regex=True) # format survey fields data types to string
        df_data.to_csv(filepath, index= False) # write to csv
    
    df_data = df_data.replace("None","")
    return df_data


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header 


def send_email(fromEmail,toEmail, subject,text, fromPass, smptpServer):
    
    msg = MIMEMultipart()
    msg['From'] = str(Header("WhatsApp Marketing <%s>" %(fromEmail)))
    msg['To'] = toEmail
    msg['Subject'] = subject
    TEXT = MIMEText(text, 'plain')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(TEXT)


    server = smtplib.SMTP(smptpServer,587)
    server.starttls()
    server.login(fromEmail, fromPass) 

    server.sendmail(fromEmail,toEmail,msg.as_string())
    server.quit()
