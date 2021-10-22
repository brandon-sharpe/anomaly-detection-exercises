####### GET CURRICULUM LOG DATA #######

import pandas as pd
import os
from env import host, user, password

def get_connection(db, username=user, host=host, password=password):
    '''
    Creates a connection URL
    '''
    return f'mysql+pymysql://{username}:{password}@{host}/{db}'
    
def new_log_data():
    '''
    Returns curriculum log info into a dataframe
    '''
    sql_query = '''  
    SELECT *
    FROM logs
    LEFT OUTER JOIN cohorts
    ON cohorts.id = logs.cohort_id;
    '''
    df = pd.read_sql(sql_query, get_connection('curriculum_logs'))
    return df 
    

def get_log_data():
    '''
    This function reads in data from Codeup database, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('log_data_df.csv'):
        
        # If csv file exists, read in data from csv file.
        df = pd.read_csv('log_data_df.csv', index_col=0)
        
    else:
        
        # Read fresh data from db into a DataFrame.
        df = new_log_data()
        
        # Write DataFrame to a csv file.
        df.to_csv('log_data_df.csv')
        
    return df

####### PREPARE FUNCTIONS FOR CURRICULUM LOG DATA ######

import pandas as pd
import numpy as np

def prep_log(df):
    '''
    Takes in df and does all preparation (see README.MD for detailed cleaning steps)
    '''
    df['date'] = df.date + ' ' + df.time
    date_cols = [
        'date',
        'start_date',
        'end_date',
        'created_at',
        'updated_at'
    ]
    df[date_cols] = df[date_cols].apply(pd.to_datetime)
    df = df.set_index(df.date)
    program_dict = {
    1 : 'Web Dev - PHP',
    2 : 'Web Dev - Java',
    3 : 'Data Science',
    4 : 'Web Dev - Front End'
    }
    df['program'] = df.program_id.map(program_dict)
    cols_to_drop = [
        'date',
        'time',
        'cohort_id',
        'slack',
        'deleted_at'
    ]
    df = df.drop(columns=cols_to_drop)
    df= df[df.name != "Staff"]
    return df