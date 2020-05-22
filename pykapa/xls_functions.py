from dateutil import parser
from datetime import datetime, timedelta
import uuid as UUID

# check if the selected field is equal to the expected value
def selected(field,value):
    sel = str(field).split(" ")
    print('Selected: %s'%sel)
    print('Value: %s' %value )
    
    if str(value) in sel:
        return True
    else:
        return False

# determine the length of the string
def string_length(field):
    return len(field)

# return the selected value at the position described by the number
def selected_at(field, number):
    field = field.split(' ')
    try:
        return field[number]
    except Exception as err:
        return err

# count the number of selected items
def count_selected(field):
    field = field.split(' ')
    return len(field)

# concatenate strings
def concat(field1, field2,*therest):
    return str(field1) + str(field2) + ''.join(map(str, therest))


#retrun substring
def substr(fieldorstring, startindex, endindex):
    if endindex<len(fieldorstring) and startindex<len(fieldorstring):
        return fieldorstring[startindex:endindex]
    else:
        return 'Error: Specified startindex/endindex is out range'    


def coalesce(field1, field2):
    if len(field1)!=0:
        return field1
    else:
        return field2


import re
#Returns true or false depending on whether the field matches the regular expression specified
def regex(field, expression):
    if re.search(expression,field) != None:
        return True
    else:
        return False

#function to execute an if statement
def IF(expression, valueiftrue, valueiffalse):
    if eval(expression)==True:
        return valueiftrue
    else:
        return valueiffalse


# change data-type to float
def number(field):
    return float(field)
# change data-type to string
def string(field):
    return str(field)


# format dates from inputs

#Converts string into a date
def date(string):
    dt = parser.parse(string).date()
    return dt
#Converts string into a date and time    
def date_time(string):
    dt = parser.parse(string)
    return dt

#Converts date and/or time into a string
def format_date_time(field, date_format):
    dt = parser.parse(str(field)).strftime(date_format)
    return dt
#today's date
def today():
    dt = datetime.now().date()
    return dt
#current date and time
def now():
    dt = datetime.now()
    return dt

def date_N_days_ago(N):
    return datetime.now() - timedelta(days=N)

# return the number of days
def days(timedelta):
    try:
        days = timedelta.days
        return days
    except:
        Obj = string(type(timedelta))
        err_msg = '*Argument Error*: days(arg) accepts datetime.timedelta arguments and not ' + Obj + '\n' +'days('+timedelta+') must take the form ' +'days(date_1 - date_2)'
        
        return err_msg


# function to check if string is float
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


#Check if argument is date
def date_check(dateX):
    try:
        dt = parser.parse(dateX).date()
        return True
    except :
        return False

# return the label for a select_one or select_multiple field choice
def jr_choice_name(value, field, df_xls):
    df_choices = df_xls['choices']
    df_select  = df_xls['select']
    
    # find the listname of the field
    df_row = df_select[df_select.name == field] # find the row containing the field
    listname = df_row.loc[df_row.index.values[0], 'list_name'] # return the listname
    
    # find the label from the choices dataframe
    df_listname = df_choices[df_choices.choice_list_name == listname] # filter the choice dataframe by the listname 
    df_list_row = df_listname[df_listname.choice_name == value] # find the row that contains the value
    
    if len(df_list_row)>0:
        label = df_list_row.loc[df_list_row.index.values[0],'choice_label'] # return the label from the row
    else:
        label = 'nan'
    
    return label

# change syntax of a xls function in a string
def str_func(s,func_name):
    
    # add open parentheses to func_name
    if func_name[-1]!='(':
        func_name = concat(func_name,'(')
    # replace colons and hyphens with underscores   
    func = func_name.replace(':','_') # replace colons
    func = func.replace('-','_') # replace hyphens

    s_right= ')' # assign closed parantheses 
    try:
        # get the argument and format the function
        sub_s = get_substring(func_name,s_right, s)
        if sub_s[-1] != ')':
            idx = sub_s.rfind(')')
            sub_s = sub_s[0:idx+1]
        new_func = concat(func,sub_s[1:len(sub_s)-1],',df_xls',')') # the correctly formated function
        
        # form new string
        new_str = s.replace(concat(func_name[0:len(func_name)-1],sub_s), new_func) # the new string with function
        return new_str
        
    except Exception as err:
        err = str(err)
        #print(string)
        return s
    
#get substring 
def get_substring(s_left,s_right, s):
    r = re.search(concat(s_left,'(.*)',s_right),s)
    return r.group(1)
# determine if sub string (sub_s) is in string (s)
def is_in(sub_s,s):
    return sub_s in s

# check for balanced parentheses in string
def balanced_par(myStr): 
    open_list = ["[","{","("] 
    close_list = ["]","}",")"]
    stack = []
    stack= []
    
    for char in myStr: 
        #print(char)
        if char in open_list: 
            stack.append(char) # append char in stack list
            
        elif char in close_list: 
            pos = close_list.index(char) # determine the index of the closed paranthesis
                        
            if len(stack) > 0 and open_list[pos] == stack[len(stack)-1]: 
                stack.pop()
            else: 
                return False
            
            if len(stack) == 0: 
                return True

# get a function from string
def get_func(string, func):
    f_idx   = string.index(func) # index of the function
    new_str = string[f_idx+len(func) : ] # new short string
    char_i = 0 # initialize char counter
    open_list = ["[","{","("]
    
    try:
        if new_str[0] in open_list: 
            for char in new_str:
                char_i +=1
                # check if there are balanced parantheses
                bal_par = balanced_par(new_str[0:char_i])
                if bal_par == True:
                    func_0 = concat(func,new_str[0:char_i])
                    return func_0
        
        
    except Exception as err:
        print(err)
        return None
                
                 

# eval functions in string               
def evalfunc_str(string,df_xls,funcs = ['jr_choice_name','date_check','count_selected','is_number','now', 
                                        'today','format_date_time','date_time','date','string','number','IF', 'regex',
                                        'coalesce','substr','concat','count_selected','selected_at','string_length',
                                        'selected','uuid','round','int']):
    nan = 'nan'
    for func in funcs:
        #print('evalFuncStr: ',string)
        occ = str(string).count(func)# count occurence of func in string
        if occ > 0:
            for i in range(occ):
                
                
                if func in string:
                    
                    func_str = get_func(string, func)
                    
                else:
                    func_str = None

                if func_str != None:
                    try:
                        result = eval(func_str) # evaluate function
                        string= string.replace(func_str, str(result)) # replace function with the evluated result
                        
                        #print('Function: %s Result: %s'%(func_str, result))
                    
                    except Exception as err:
                        print(err)
                        string = string
    return string          

# change the syntax of xls function to pyhton               
def format_funcstr(string,func):
    occ = string.count(func)# count occurence of func in string
    
    if occ > 0:
        for i in range(occ):
            #print('occ: ',i)
            if func in string:
                func_str = get_func(string, func)
            else:
                func_str = None
                
            if func_str != None:
                
                arg    = func_str[len(func): len(func_str)]
                new_arg= concat(arg[0:len(arg)-1],', df_xls)')

                #print('index: ',i, ' o_arg: ', arg, ' n_arg: ', new_arg)
                
                new_func = func.replace(':','_')
                new_func = new_func.replace('-','_')
                
                string = string.replace(concat(func,arg),concat(new_func,new_arg))
                #print(string)
                
    #print('ffs: ', string)

    return string 

def uuid():
    return str(UUID.uuid4())    