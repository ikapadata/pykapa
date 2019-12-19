import requests
from xls_functions import concat, format_date_time, evalfunc_str, is_in, now, uuid
import json
import requests
import time
import os
import ast
import pandas as pd
from copy import deepcopy
import re

from gen_funcs import *
#--------------------------------------------------------------------------------------------------------------------------------
#                                                   sim control functions
#--------------------------------------------------------------------------------------------------------------------------------
# form string into json query
def json_query(string):
    dict1= {'query':string} # form dictionary
    json_query = json.dumps(dict1) # convert dictionary into json
    return json_query

# api requests
def api_query(string,api_key):
    api_key = str(api_key)
    json_qry = json_query(string) # convert string into json data
    api_url ='https://new.simcontrol.co.za/graphql/'
    headers = {'Content-Type': 'application/json', 'simcontrol-api-key': api_key}
    api_qry = requests.post(url = api_url, headers = headers, data = json_qry)           # api_qry
    
    return api_qry.json()

# update internal database
def append_json_db(filepath,data):
    incentive_db = read_json_file(filepath) # read t[he internal db
    incentive_db.append(data) # append current recharge data to internal db in json file
    write_to_json(filepath, incentive_db) # write to the internal db

# metadata for a specific recharge
def recharge_data(api_key, reference):
    # form query string
    string = concat('{ adhocRecharges(first: 1, reference: \"',reference,'\") { edges { node {id msisdn network { name } productType product { label } price status succeededAt created failureType reference} } } }')
    data = api_query(string,api_key)['data']['adhocRecharges']['edges']
        
    if len(data) != 0:
        data = data[0]['node']
        data['network'] = data['network']['name']
        
    return data

# status of recharge
def recharge_status(api_key, reference):
    status = recharge_data(api_key, reference)['status']
    return status

# simcontrol balance
def sim_control_balance(api_key):
    # check the remaining account balance
    string = "{ account { name balance } }"
    bal_json = api_query(string,api_key)
    
    return bal_json['data']['account'][0]['balance']

# list of mobile network operators
def sim_control_networks(api_key):
   
    string = "{ networks {id name } }"
    api_resp = api_query(string,api_key)
    mno = api_resp['data']['networks'] # API response
    
    #create a dcitionary of the available networks
    networks = {}
    for i in range(len(mno)):
        networks[mno[i]['name'].lower()] = mno[i]['id']
        
    return networks


# format contact for simcontrol
def simcontact(msisdn):
    msisdn = str(msisdn)

    if len(msisdn)==11 and '.' in msisdn and msisdn.index('.')==9:
        idx = msisdn.index('.')
        sub_1 = msisdn[0:9]
        msisdn = msisdn[-1] + sub_1

    contact = remove_specialchar(msisdn) # remove special characters
    # format contact
    if contact[0] =='0' and len(contact) == 10:
        return concat('+27',contact[1:len(contact)])
    
    elif contact[0:2] == '27' and len(contact[2:len(contact)])==9:
        return concat('+',contact)
    
    elif len(contact)== 9 and contact[0]!= '0':
        return concat('0', contact)
        
    else:
        return concat(msisdn,' is not recognised as a South African mobile number.')

    
#count the number of recharges per contact
def recharge_count(contact,api_key, prodType = 'airtime', status = 'success', r_idx = 100, startDate = '', endDate=''):

    # create query string
    if startDate == '' or endDate == '':
        q_string = concat('{ adhocRecharges( first: ',r_idx,', msisdn: \"',simcontact(contact),'\", productType: \"',prodType.upper(),'\", status: \"',status.upper(),'\") { edges { node {id msisdn network { name } productType product { label } price status succeededAt created failureType reference} } } }')
    
    else:
        # format startDate and endDate
        fmt = '%Y-%m-%dT%H:%M:%S'
        startDate = format_date_time(str(startDate),fmt)
        endDate = format_date_time(str(endDate),fmt)
        q_string = concat('{ adhocRecharges( first: ',r_idx,', msisdn: \"',simcontact(contact),'\", productType: \"',prodType.upper(),'\", status: \"',status.upper(),'\", created_Gte:\"',startDate,'\" created_Lt:\"',endDate,'\") { edges { node {id msisdn network { name } productType product { label } price status succeededAt created reference } } } }')
   
    # prequest the 1st 100 recharges fot the msisdn
    q_res = api_query(q_string,api_key) # perform api_query
    #print(q_res)
    #count number of successful recharges
    recharges = str(q_res).count(simcontact(contact))
    
    return recharges

# Define 'airtime' function to send airtime to msisidn and return metadata
def airtime(api_key, msisdn, network, amount, ref=None):
    if ref == None:
        ref = str(uuid())
    
    if msisdn != 'nan' and network != 'nan' and amount != 'nan':
        # a. Determine the network ID for a given network name
        try:
            mno_id = sim_control_networks(api_key)[network.lower()] #retrieve network ID  
        except:
            mno_id = 'TmV0d29ya05vZGU6MTM='
        # b. form query_string and query simcontrol API
        string = concat('mutation { rechargeSim(msisdn: \"',simcontact(msisdn),'\", networkId: \"',mno_id,'\", airtimeAmount:',amount,', reference: \"',ref, '\") { rechargeId message}}')
        recharge = api_query(string,api_key) # perform api_query
        
        # c. request recharge data
        data_recharge = [recharge_data(reference=ref,api_key=api_key)] # get metadata data of recharge 
    
        return pd.DataFrame(data_recharge)
    
# recharge failed recharges
def recharge_failed(api_key, msisdn, ref=None, startDate = None, endDate= str(now()), recharge_limit = 1, project = None, prodType = 'airtime'):
    # a. obtain history of recharges for the given msisdn
    if endDate != None and startDate != None:
        df_rec = msisdn_history(api_key, msisdn, startDate = startDate,  prodType = prodType)
    else:
        df_rec = msisdn_history(api_key, msisdn, prodType = prodType)
    
    if str(df_rec) != 'None':
        print('prodType: ', prodType,'\t MSISDN:',msisdn, '\t HISTORY: ',len(df_rec))
        # b. obtain successful and failed recharges
        if project != None:
            s_rec = df_rec[df_rec['reference'].str.contains(project) & df_rec['status'].str.contains('SUCCESS')] # records of successful recharges in the given project
            f_rec = df_rec[df_rec['reference'].str.contains(project) & df_rec['status'].str.contains('FAILED')]  # records of FAILED recharges in the given project
        else: 
            s_rec = df_rec[df_rec['status'].str.contains('SUCCESS')] # records of successful recharges
            f_rec = df_rec[df_rec['status'].str.contains('FAILED')]  # records of failed recharges
 
        if len(s_rec) < recharge_limit and len(f_rec) >0 and len(f_rec) <= 1: 
            # recharge msisdn   
            if f_rec.loc[0,'productType'] == 'AIRTIME':
                recharge = airtime(api_key, msisdn, network = f_rec.loc[0,'network']['name'] , amount = f_rec.loc[0,'price'], ref= concat(f_rec.loc[0,'reference'],'_FINALTRIAL'))

                return recharge
            else:
                recharge = buyProd(api_key, msisdn, network = f_rec.loc[0,'network']['name'] , prodID = f_rec.loc[0,'product']['id'], ref= concat(f_rec.loc[0,'reference'],'_FINALTRIAL'))
                return recharge
        
            
    else:
        return None


# format dates to suit simcontrol     
def sim_dateformat( startDate = None, endDate = None, fmt = '%Y-%m-%dT%H:%M:%S' ):
    if startDate != None and endDate != None:
        startDate = format_date_time(str(startDate),fmt)
        endDate = format_date_time(str(endDate),fmt)
        return {'startDate': startDate, 'endDate' : endDate}
    else:
        return None

# convert simcontrol's dictionary to DataFrame
def simdict_to_DataFrame(simdict):
    rec_lst = []
    for rec in simdict:
        rec = rec['node']
        rec_lst.append(rec)
        
    return pd.DataFrame(rec_lst) # convert to dataframe


# Def 'msisdn_history': obtain the a list of the first 100 records of the given msisdn
def msisdn_history(api_key, msisdn, prodType = 'airtime', status = None, r_idx = 100, startDate = None, endDate= str(now())):
    try:
        # a. create query string with and without time period filter
        if startDate != None and endDate != None:
            # a(i). form query string with time period filter
            date = sim_dateformat( startDate = startDate, endDate = endDate, fmt = '%Y-%m-%dT%H:%M:%S' ) # format startDate and endDate
            if status != None:
                q_string = concat('{ adhocRecharges( first: ', r_idx,', msisdn: \"', simcontact(msisdn),'\", productType: \"', prodType.upper(),'\", status: \"', status.upper(),'\", created_Gte:\"', date['startDate'],'\" created_Lt:\"', date['endDate'],'\") { edges { node {id msisdn network { name } productType product { label id} price status succeededAt created reference } } } }')
            else:
                q_string = concat('{ adhocRecharges( first: ', r_idx,', msisdn: \"', simcontact(msisdn),'\", productType: \"', prodType.upper(),'\", created_Gte:\"', date['startDate'],'\" created_Lt:\"', date['endDate'],'\") { edges { node {id msisdn network { name } productType product { label id} price status succeededAt created reference } } } }')

        else:
            # a(ii). query string without time period filter
            if status != None:
                q_string = concat('{ adhocRecharges( first: ', r_idx,', msisdn: \"', simcontact(msisdn),'\", productType: \"', prodType.upper(),'\", status: \"', status.upper(),'\") { edges { node {id msisdn network { name } productType product { label id} price status succeededAt created failureType reference} } } }')
            else:
                q_string = concat('{ adhocRecharges( first: ', r_idx,', msisdn: \"', simcontact(msisdn),'\", productType: \"', prodType.upper(),'\") { edges { node {id msisdn network { name } productType product { label id} price status succeededAt created failureType reference} } } }')

        # b. obtain msisdn recharge history
        r_hist = simdict_to_DataFrame(api_query(q_string,api_key)['data']['adhocRecharges']['edges']) # perform api_query
        
        if len(r_hist) < 1 or len(list(r_hist))<1:
            r_hist = []
        
    except Exception as err:
        print(str(err))
        
        r_hist = None
    
    return r_hist

# recharge failed recharges in given project
def project_recharge_failed(api_key, project, recharge_limit = 1, prodType = 'airtime'):
    dirX = make_relative_dir('data', project, 'qctrack.json')
    qc_track = read_json_file(dirX)
    finaltrial = qc_track['finalTrial']

    msisdn_lst = qc_track['failedRecharges']
    lst = deepcopy(msisdn_lst)
    
    for msisdn in msisdn_lst:
        recharge = recharge_failed(api_key, msisdn, project = project, prodType = prodType)
        if str(recharge) != str(None) and recharge.loc[0,'status']=='SUCCESS':
            lst.remove(msisdn)
        else:
            lst.remove(msisdn)
            finaltrial.append(simcontact(msisdn))
            

    # update tracker
    qc_track['failedRecharges'] = lst
    qc_track['finalTrial'] = finaltrial
    write_to_json(dirX, qc_track) # record the last checked interview in json file
       

# list of availble data or sms bundles (default is DATA)
def products( api_key, network, prodType = 'DATA', idx = 100):
    # 2. api query
    if network.lower() == 'mtn':
        network = network.upper()
    else:
        network = network.title()
        
    string = concat('{  products(first:',idx, 'productType: \"', prodType.upper(), '\" network_Name:\"', network,'\"){edges{node{ network{id name} id productType label price bundleSize bundleSizeInMb}} }}')
    df_resp = simdict_to_DataFrame(api_query(string,api_key)['data']['products']['edges']).sort_values(by=['price','bundleSize'])
    return df_resp


# find product or top 3 closest based on the data bundle size
def dataProducts(bundleSize, df):
    # format string
    req = re.sub('[^.,a-zA-Z0-9]', '', str(bundleSize).upper()) # remove special chars except (,) and (.)
    req = re.compile('(?<=\d),(?=\d)').sub('.',req.upper()).replace(' ','') # a. change decimal point from (,) to period (.)

    # b. get the float from the bundleSize string.
    if 'GB' in req:
        val = req[0:req.index('GB')]
        sfx = 'GB'
    elif 'MB' in req:
        val = req[0:req.index('MB')]
        sfx = 'MB'
    try:
        val = float(val)
    except:
        val = None

    # c. find the value the float is closest to
    if str(val) != str(None):
        if 'GB' in req:
            if val >= 1:
                df = df.loc[df.bundleSizeInMb>=1000]
                close_row = df.iloc[(df['bundleSize']-val).abs().argsort()[:3]]
            else:
                df = df.loc[df.bundleSizeInMb<1000]
                close_row = df.iloc[(df['bundleSize']-val*1000).abs().argsort()[:3]]
        elif 'MB' in req:

            if str(val) != str(None) and val < 1000:
                df = df.loc[df.bundleSizeInMb<1000]
                close_row = df.iloc[(df['bundleSize']-val).abs().argsort()[:3]]
            else:
                df = df.loc[df.bundleSizeInMb>1000]
                close_row = df.iloc[(df['bundleSize']-val/1000).abs().argsort()[:3]]

        # products close to bundleSize.
        bundleSize = close_row.loc[close_row.index.values[0],'bundleSize']
        if bundleSize == val:
            row = close_row[close_row.bundleSize == val]
            print('===')
            return row[['bundleSize','price','label','id']]
        else:
            
            return close_row[['bundleSize','price','label','id']]
        
# find product or top 3 closest based on the sms bundle size
def smsProducts(bundleSize, df):
    # format string
    req = re.sub('[^.,a-zA-Z0-9]', '', str(bundleSize).upper()) # remove special chars except (,) and (.)
    req = re.compile('(?<=\d),(?=\d)').sub('.',req.upper()).replace(' ','') # a. change decimal point from (,) to period (.)

    # b. get the float from the bundleSize string.
    if 'SMS' in req:
        val = req[0:req.index('SMS')]
        sfx = 'SMS'
    try:
        val = float(val)
    except:
        val = None

    # c. find the value the float is closest to
    if str(val) != str(None):
        close_row = df.iloc[(df['bundleSize']-val).abs().argsort()[:3]]

        # products close to bundleSize.
        bundleSize = close_row.loc[close_row.index.values[0],'bundleSize']
        if bundleSize == val:
            row = close_row[close_row.bundleSize == val]
            return row[['bundleSize','price','label','id']]
        else:
            return close_row[['bundleSize','productType','price','label','id']]   
# Buy DATA or SMS bundles
def buyProd(api_key, msisdn, network, prodID, ref= None):
    if ref == None:
        ref = str(uuid())
        
    if msisdn != 'nan' and network != 'nan':  
        # a. Determine the network ID for a given network name
        try:
            mno_id = sim_control_networks(api_key)[network.lower()] #retrieve network ID
        except:
            mno_id = 'TmV0d29ya05vZGU6MTM='
        # b. form query_string and query simcontrol API
        string = concat('mutation { rechargeSim(msisdn: \"',simcontact(msisdn),'\", networkId: \"',mno_id,'\", productId:\"',prodID,'\", reference: \"',ref, '\") { rechargeId message}}')
        recharge = api_query(string,api_key) # perform api_query
        # c. request recharge data
        data_recharge = [recharge_data(reference=ref,api_key=api_key)] # get metadata data of recharge
        
        print(data_recharge)
        return pd.DataFrame(data_recharge)
    
# recharge sim card with airtime, data or sms
def rechargeSim(api_key, msisdn, network, prodType, bundleSize=None, price = 1, ref=None, prodID = None ,buy='auto'):
    
    if ref == None:
        ref = str(uuid())
        
    prodType = prodType.upper()
    if prodType == 'AIRTIME':
        resp = airtime(api_key = api_key, msisdn = msisdn, network = network, amount = price, ref=ref)
        return resp
        
    else:
        if prodID == None:
            df   = products( api_key = api_key, network= network, prodType = prodType)
            print('AVAILABLE: \n',df)
            
            if prodType == 'DATA' and bundleSize != None and ('MB' in bundleSize.upper() or 'GB' in bundleSize.upper()):
                prod = dataProducts(bundleSize, df)
                
                print('\nPRODUCTS: \n',prod)
                
            elif prodType == 'SMS' and bundleSize != None:
                prod  = smsProducts(bundleSize, df)
            else:
                prod = None
            
            if str(prod) != str(None) and buy.lower() == 'auto':
                print('Buying product')
                prodID = prod.loc[prod.index.values[0],'id']
                print('prodID:',prodID)
                resp = buyProd(api_key, msisdn, network, prodID, ref)
                return resp
            else:
                print('No purchase')
                print(len(prod))
                return prod
        else:
            print('Buying product')
            resp = buyProd(api_key, msisdn, network, prodID, ref)
            return resp
