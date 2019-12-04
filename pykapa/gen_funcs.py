import os
import json

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
    
    return {'SHEET_LINK':google_sheet_url,'EMAIL':email,'PASSWORD':password,'SERVER':server, 'ERR_CHNL':err_chnl}

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