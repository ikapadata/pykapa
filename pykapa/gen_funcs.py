import os
import json
import pandas as pd
from pykapa.settings.logging import logger


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
def append_to_csv(filepath, df_survey):
    if os.path.isfile(filepath):
        logger.info('APPENDING NEW CSV:')
        df_0 = pd.read_csv(filepath)
        # append unique data
        df_ = pd.concat([df_0, df_survey]).drop_duplicates(subset=['KEY'], keep='first')[list(df_survey)]
        df_str = df_.astype(str).replace('nan', '', regex=True)
        df_str.to_csv(filepath, index=False)
    else:
        logger.info('WRITING NEW CSV:')
        df_str = df_survey.astype(str).replace('nan', '', regex=True)
        df_str.to_csv(filepath, index=False)

    df_data = df_str.replace("None", "")
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
