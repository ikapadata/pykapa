"""Upload the contents of your Downloads folder to Dropbox.
This is an example app for API v2.
"""

from __future__ import print_function

import argparse
import contextlib
import datetime
import os
import six
import sys
import time
import unicodedata


from quality_functions import * #df_worksheet, slack_post,timezone_sast, surveyCTO_download,open_google_sheet, local_csv
from xls_functions import *
from incentives_functions import *
from gen_funcs import *
from gspread_dataframe import get_as_dataframe
import requests
import base64
from airtable import Airtable

#from quality_functions import timezone_sast

if sys.version.startswith('2'):
    input = raw_input  # noqa: E501,F821; pylint: disable=redefined-builtin,undefined-variable,useless-suppression

import dropbox

#get user id to send in messages
def slack_user_id(string):
    json_bot = read_json_file('./data/authentication/slack/bot_token.json')
    bot_token = json_bot['BOT_TOKEN']
    sc = slackclient.SlackClient(bot_token)
    
    users = sc.api_call("users.list")['members']
    df =  pd.DataFrame(users)
    for i in df.index.values:
        string = string.replace("<@%s>"%df.at[i,'name'],"<@%s>"%df.at[i,'id'])
        
    return string

#function to post messages to slack
def slack_post(channel_name, slack_msg): 
    json_bot = read_json_file('./data/authentication/slack/bot_token.json')
    bot_token = json_bot['BOT_TOKEN']
    slack_client = slackclient.SlackClient(bot_token)
    try:
        slk_txt = slack_user_id(slack_msg)
    except Exception as err:
        print(err)
        slk_txt  = slack_msg
    slack_client.api_call('chat.postMessage', channel=str(channel_name).lower(), text=slk_txt)
    
#convert UTC to SAST timezone
def timezone_sast(date_str):
    dt = date_time(date_str)
    fmt = '%b %d, %Y   %H:%M:%S'
    utc_dt = pytz.utc.localize(dt)
    #convert utc to jhb timezone
    jhb = pytz.timezone('Africa/Johannesburg')
    return  utc_dt.astimezone(jhb).strftime(fmt)

def dbx_parse(TOKEN):
    parser = argparse.ArgumentParser(description='Sync ~/Downloads to Dropbox')
    parser.add_argument('folder', nargs='?', default='Downloads',
                        help='Folder name in your Dropbox')
    parser.add_argument('rootdir', nargs='?', default='~/Downloads',
                        help='Local directory to upload')
    parser.add_argument('--token', default=TOKEN,
                        help='Access token '
                        '(see https://www.dropbox.com/developers/apps)')
    parser.add_argument('--yes', '-y', action='store_true',
                        help='Answer yes to all questions')
    parser.add_argument('--no', '-n', action='store_true',
                        help='Answer no to all questions')
    parser.add_argument('--default', '-d', action='store_true',
                        help='Take default answer on all questions')
    
    return parser


def main():
    """Main program.
    Parse command line, then iterate over files and directories under
    rootdir and upload all files.  Skips some temporary files and
    directories, and avoids duplicate uploads by comparing size and
    mtime with the server.
    """
    args = parser.parse_args()
    if sum([bool(b) for b in (args.yes, args.no, args.default)]) > 1:
        print('At most one of --yes, --no, --default is allowed')
        sys.exit(2)
    if not args.token:
        print('--token is mandatory')
        sys.exit(2)

    folder = args.folder
    rootdir = os.path.expanduser(args.rootdir)
    print('Dropbox folder name:', folder)
    print('Local directory:', rootdir)
    if not os.path.exists(rootdir):
        print(rootdir, 'does not exist on your filesystem')
        sys.exit(1)
    elif not os.path.isdir(rootdir):
        print(rootdir, 'is not a folder on your filesystem')
        sys.exit(1)

    dbx = dropbox.Dropbox(args.token)

    for dn, dirs, files in os.walk(rootdir):
        subfolder = dn[len(rootdir):].strip(os.path.sep)
        listing = list_folder(dbx, folder, subfolder)
        print('Descending into', subfolder, '...')

        # First do all the files.
        for name in files:
            fullname = os.path.join(dn, name)
            if not isinstance(name, six.text_type):
                name = name.decode('utf-8')
            nname = unicodedata.normalize('NFC', name)
            if name.startswith('.'):
                print('Skipping dot file:', name)
            elif name.startswith('@') or name.endswith('~'):
                print('Skipping temporary file:', name)
            elif name.endswith('.pyc') or name.endswith('.pyo'):
                print('Skipping generated file:', name)
            elif nname in listing:
                md = listing[nname]
                mtime = os.path.getmtime(fullname)
                mtime_dt = datetime.datetime(*time.gmtime(mtime)[:6])
                size = os.path.getsize(fullname)
                if (isinstance(md, dropbox.files.FileMetadata) and
                        mtime_dt == md.client_modified and size == md.size):
                    print(name, 'is already synced [stats match]')
                else:
                    print(name, 'exists with different stats, downloading')
                    res = download(dbx, folder, subfolder, name)
                    with open(fullname) as f:
                        data = f.read()
                    if res == data:
                        print(name, 'is already synced [content match]')
                    else:
                        print(name, 'has changed since last sync')
                        if yesno('Refresh %s' % name, False, args):
                            upload(dbx, fullname, folder, subfolder, name,
                                   overwrite=True)
            elif yesno('Upload %s' % name, True, args):
                upload(dbx, fullname, folder, subfolder, name)

        # Then choose which subdirectories to traverse.
        keep = []
        for name in dirs:
            if name.startswith('.'):
                print('Skipping dot directory:', name)
            elif name.startswith('@') or name.endswith('~'):
                print('Skipping temporary directory:', name)
            elif name == '__pycache__':
                print('Skipping generated directory:', name)
            elif yesno('Descend into %s' % name, True, args):
                print('Keeping directory:', name)
                keep.append(name)
            else:
                print('OK, skipping directory:', name)
        dirs[:] = keep

def list_folder(dbx, folder, subfolder):
    """List a folder.
    Return a dict mapping unicode filenames to
    FileMetadata|FolderMetadata entries.
    """
    path = '/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'))
    while '//' in path:
        path = path.replace('//', '/')
    path = path.rstrip('/')
    try:
        with stopwatch('list_folder'):
            res = dbx.files_list_folder(path)
    except dropbox.exceptions.ApiError as err:
        print('Folder listing failed for', path, '-- assumed empty:', err)
        return {}
    else:
        rv = {}
        for entry in res.entries:
            rv[entry.name] = entry
        return rv

def download(dbx, folder, subfolder, name):
    """Download a file.
    Return the bytes of the file, or None if it doesn't exist.
    """
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    while '//' in path:
        path = path.replace('//', '/')
    with stopwatch('download'):
        try:
            md, res = dbx.files_download(path)
        except dropbox.exceptions.HttpError as err:
            print('*** HTTP error', err)
            return None
    data = res.content
    print(len(data), 'bytes; md:', md)
    return data

def upload(dbx, fullname, folder, subfolder, name, overwrite=True):
    """Upload a file.
    Return the request response, or None in case of error.
    """
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    #print('dbx path: ',path)
    #print('dbx: ',dbx)
    print('LOCAL FILEPATH: %s'%fullname)
    while '//' in path:
        path = path.replace('//', '/')
    mode = (dropbox.files.WriteMode.overwrite
            if overwrite
            else dropbox.files.WriteMode.add)
    mtime = os.path.getmtime(fullname)
    
    print('DROPBOX FILEPATH: %s\n' %path)
    with open(fullname, 'rb') as f:
        data = f.read()
    with stopwatch('upload %d bytes' % len(data)):
        try:
            
            
            res = dbx.files_upload(
                data, path, mode,
                client_modified=datetime(*time.gmtime(mtime)[:6]),
                mute=True)
            
        except dropbox.exceptions.ApiError as err:
            print('*** API error', err)
            return None
    print('uploaded as', res.name.encode('utf8'))
    return res

def yesno(message, default, args):
    """Handy helper function to ask a yes/no question.
    Command line arguments --yes or --no force the answer;
    --default to force the default answer.
    Otherwise a blank line returns the default, and answering
    y/yes or n/no returns True or False.
    Retry on unrecognized answer.
    Special answers:
    - q or quit exits the program
    - p or pdb invokes the debugger
    """
    if args.default:
        print(message + '? [auto]', 'Y' if default else 'N')
        return default
    if args.yes:
        print(message + '? [auto] YES')
        return True
    if args.no:
        print(message + '? [auto] NO')
        return False
    if default:
        message += '? [Y/n] '
    else:
        message += '? [N/y] '
    while True:
        answer = input(message).strip().lower()
        if not answer:
            return default
        if answer in ('y', 'yes'):
            return True
        if answer in ('n', 'no'):
            return False
        if answer in ('q', 'quit'):
            print('Exit')
            raise SystemExit(0)
        if answer in ('p', 'pdb'):
            import pdb
            pdb.set_trace()
        print('Please answer YES or NO.')

@contextlib.contextmanager
def stopwatch(message):
    """Context manager to print how long a block of code took."""
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        print('Total elapsed time for %s: %.3f' % (message, t1 - t0))

# upload files to dropbox
def dropbox_upload(dir_local,dir_dropbox,dbx_token):
    """Main program.
    Parse command line, then iterate over files and directories under
    rootdir and upload all files.  Skips some temporary files and
    directories, and avoids duplicate uploads by comparing size and
    mtime with the server.
    """
    parser = dbx_parse(dbx_token)
    args = parser.parse_args()
    #print("c. Passed Parser")
    if sum([bool(b) for b in (args.yes, args.no, args.default)]) > 1:
        print('At most one of --yes, --no, --default is allowed')
        sys.exit(2)
    if not args.token:
        print('--token is mandatory')
        sys.exit(2)


    folder  = dir_dropbox #args.folder
    rootdir = dir_local #os.path.expanduser(args.rootdir)

    dbx = dropbox.Dropbox(dbx_token)

    for dn, dirs, files in os.walk(rootdir):
        
        subfolder = dn[len(rootdir):].strip(os.path.sep)
        #listing = list_folder(dbx, folder, subfolder)
        print('Descending into', subfolder, '...')
        #print('dn: ',dn)
        #print(files)
        
        # First do all the files.
        for name in files:
            
            fullname = os.path.join(dn, name)
            if '.json' not in name and '.DS_Store' not in name:
                upload(dbx, fullname, folder, subfolder, name)

# dropbox upload function
def to_dropbox(apikey, path, form_id, total_recs = None, recs = None, err_chnl=None, msg_head = ""):
    
    if msg_head == "":
        msg_head = "DROPBOX EXPORT" 

               
    dir_lcl = './data/projects/%s'% form_id
    if not pd.isnull(apikey) and os.path.exists(dir_lcl):
        #print('\nCalling Drop Box upload function')
        dropbox_token = apikey.strip()
        
        prefix = '!k4p4D4T4+'
        if prefix in dropbox_token:
            db_token = dropbox_token.replace(prefix,"")
            token = base64.b64decode(db_token).decode("utf-8")
            
        else:
            token = dropbox_token
            
        print("\nToken: ",token)
            
        #slack_post(err_chnl, 'Input: %s, Output: %s' %(dropbox_token,token) )
        
        dropbox_upload( dir_local = dir_lcl, dir_dropbox = path, dbx_token = token)
        
        date = timezone_sast(str(now()))
        
        slk_msg = "*%s:* \n_File_: %s.csv \n_Directory_: %s \n_New Submissions_: %s \n_Total Submissions_: %s\n_Date_: %s" %(msg_head,form_id, path, recs,total_recs, date)
        print(slk_msg)
        
        slack_post(str(err_chnl),slk_msg)
        
    else:
        print("`Dropbox:` This dataset is not backed-up on Dropbox. It is advised that you back it up on Dropbox to easily share it with stakeholders.")
           
# manually upload csv to dropbox
def manual_export(input_text, channel):
    
    # get surveycto credentials
    if os.path.isfile('./data/authentication/surveycto/scto_credentials.json'):
        json_scto = read_json_file('./data/authentication/surveycto/scto_credentials.json')
        email = json_scto['EMAIL']
        password = json_scto['PASSWORD']
        server = "http://%s"%json_scto['SERVER']
    else:
        print('Missing surveyCTO credentials')


    try:
        r = requests.get(input_text)
        gsheet = open_google_sheet(input_text) # open google sheet
        ws_sett = gsheet.worksheet('settings') # read the settings worksheet
        df_sett =  get_as_dataframe(ws_sett, evaluate_formulas=False) # read the worksheet as a dataframe
        form_id = df_sett.at[0,'form_id'] # get the form ID
    
    except Exception as err:
        form_id = input_text
        qctrack = read_json_file('./data/projects/%s/qctrack.json'%form_id)
        link = qctrack['GoogleSheet']
        gsheet = open_google_sheet(link) # open google sheet
        
       
    #slack_post(channel, form_id)
    
    df_survey = surveyCTO_download(server, email, password, form_id) # download latest data from surveycto
    print(len(df_survey))
    filepath = './data/projects/%s/%s.csv' % (form_id, form_id)
    print(filepath)
    # append new data and save locally
    df_data = local_csv(filepath, df_survey)

    # send dataset to dropbox
    print("\n2. Ready To Upload CSV To Dropbox")
    to_dropbox(gsheet, form_id, total_recs = len(df_data), recs = len(df_survey), err_chnl= channel, msg_head='MANUAL DROPBOX EXPORT')
    
    return df_data





# determine export fields
def export_fields(df, export_type, export_field):
    df_x = df.dropna(subset=[export_field])[['name',export_field]] # drop empty cells in export field
    fields = ['KEY'] + list(df_x[df_x[export_field].str.contains(export_type)]['name']) # fields for export
    return fields

# select fields relevant for export data
def select_export_data(df_data, df, export_type, export_field = 'pykapa_export'):
    fields = export_fields(df,export_type,export_field)
    print("\nFIELDS:\n%s" %fields)
    
    all_fields =[]
    for field in fields:
        export_cols = [col for col in df_data.columns if field in col and col.index(field)==0]
        all_fields = all_fields + export_cols
        

    return df_data[ all_fields ]

def create_dataframe(recs):
    lst =[]
    for i in range(len(recs)):
        lst.append(recs[i]['fields'])

    return pd.DataFrame(lst)

# get data that will be exported
def export_data(df_data, df, export_type,relevance, export_field = 'pykapa_export', select_fields = 'select'):
    
    # filter data set by relevance
    if not pd.isnull(relevance):
        df_data_flt = filter_by_relevance(relevance, df_data)
    else:
        df_data_flt = df_data
        
    # select necessary fields
    if str(select_fields).lower().strip() == 'select':
        df_export = select_export_data(df_data_flt, df, export_type)
    else:
        df_export = df_data_flt
        
    return df_export

# export paramaters for the application
def export_parameters(df_xpt,export_type):
    idx = df_xpt[df_xpt['channel'] == export_type].index.values[0]

    fields = df_xpt.at[idx,'fields']
    relevance = df_xpt.at[idx,'relevance']

    name = df_xpt.at[idx,'name']
    api_key = df_xpt.at[idx,'api_key']
    path = str(df_xpt.at[idx,'path']).replace('https://api.airtable.com/v0/','')
    view = df_xpt.at[idx,'view']
    
    return {'fields':fields,'relevance':relevance,'name':name,'api_key':api_key,'path':path,'view':view}

# export data to airtable
def to_airtable(df_data_xpt, table_name, apikey, base_key,view):
    
    prefix = '!k4p4D4T4+'
    if prefix in apikey:
        enc_token = apikey.replace(prefix,"")
        apikey = base64.b64decode(enc_token).decode("utf-8")

    
    airtable = Airtable(base_key, table_name, api_key=apikey) # connect to table
    recs = airtable.get_all(view = view) # get all records in table
    #print(recs)
    if recs != []:
        df_tbl = create_dataframe(recs) # dataframe of records in the table
        remove_list = list(df_tbl['KEY'])
        df_cln = df_data_xpt[~df_data_xpt['KEY'].isin(remove_list)] 
        #df_cln.fillna("", inplace=True)
    else:
        df_cln = df_data_xpt
    
    for i in df_cln.index.values:
        
        for col in df_cln.columns:
            value = df_cln.at[i,col]
            if date_check(value)== True and is_number(value)==False:
                value = timezone_sast(value)
                df_cln.at[i,col] = value
        
        records = df_cln.loc[[i]].dropna(axis=1).to_dict(orient = 'records')[0]
        #print(records)
        airtable.insert(records, typecast=True)