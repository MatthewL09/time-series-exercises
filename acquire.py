import pandas as pd
import numpy as np
import requests
import os

############################################################

def API_doc(base_url):
    '''
THIS FUNCTION TAKES IN A BASE URL AND PRINTS THE API DOCUMENTATION. YOU CAN USE IT TO LOOK AT THE
URL's ENDPOINTS.    
    '''

    response = requests.get(base_url + '/documentation')
    print(response.json()['payload'])

############################################################

def acquire_stores():
    '''
THIS FUNCTION TAKES IN THREE URLS AND A BASE URL TO BE USED IN GETTING MULTIPLE PAGES OF 
INFORMATION, AND CREATES A DF FOR EACH, THEN WRITING EACH DF TO CSV FILES.    
    '''

    base_url = 'https://python.zgulde.net'

    #url1 | ITEMS DATA 
    url1 = 'https://python.zgulde.net/api/v1/items'
    response = requests.get(url1)
    data = response.json()
    df = pd.DataFrame(data['payload']['items'])

    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload']['items'])])#.reset_index(drop = True)

    df_items = df.copy
    print('✅ Items data acquired')

    #url2 | STORES DATA
    url2 = 'https://python.zgulde.net/api/v1/stores'
    response = requests.get(url2)
    data = response.json()
    df = pd.DataFrame(data['payload']['stores'])

    df_stores = df.copy()
    print('✅ Stores data acquired')

    #url3 | SALES DATA
    url3 = 'https://python.zgulde.net/api/v1/sales'
    response = requests.get(url3)
    data = response.json()
    df = pd.DataFrame(data['payload']['sales'])

    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload']['sales'])])#.reset_index(drop = True)

    df_sales = df.copy()
    print('✅ Sales data acquired')

    #saving dfs to csv files
    df_items.to_csv('items.csv')#, index = False)
    df_stores.to_csv('stores.csv')#, index = False)
    df_sales.to_csv('sales.csv')#, index = False)

    return df_items, df_stores, df_sales

############################################################

def get_stores():
    '''
THIS FUNCTION CHECKS TO SEE IF CSV FILES EXISTS FOR THE STORES DATA AND, IF SO, 
WRITES THE CSV FILES TO DFS. IF NOT THE FUNCTION WILL REUN THE PREVIOUS acquire_stores()
FUNCTION.
    '''

    #checking to see if csv files exist
    if (os.path.isfile('items.csv') == False) or (os.path.isfile('stores.csv') == False) or (os.path.isfile('sales.csv') == False):
        print('Data is not cached. Acquiring new data . . .')
        df_items, df_stores, df_sales = acquire_stores()
        #if no local csv exists, running above function to write to df and cache

    else:
        print('Data is cached. Reading from csv files.')
        
        df_items = pd.read_csv('items.csv')
        print('✅ Items data acquired')

        df_stores = pd.read_csv('stores.csv')
        print('✅ Items data acquired')

        df_sales = pd.read_csv('sales.csv')
        print('✅ Sales data acquired')

    df_combined = pd.merge(df_items,
                            df_sales,
                            how = 'right',
                            right_on = 'item',
                            left_on = 'item_id')
    df_combined = pd.merge(df_combined,
                            df_stores,
                            how = 'left',
                            left_on = 'store',
                            right_on = 'store_id')
    print(f'{"*" * len("✅Acquisition Complete")}\n✅Acquisition Complete')

    #removing index cols
    df_combined.drop(columns = ['Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0'], inplace = True)

    #caching combined df
    df_combined.to_csv('combined.csv')

    return df_combined

############################################################

def acquire_power():
    '''
 THIS FUNCTION RETURNS A DF OF THE OPEN POWER SYSTEMS (OPSD) BY ACQUIRING THE DATA, READING IT TO A DF AND THEN 
 CACHING THAT DATA LOCALLY. THE PRINT STATEMENTS CONFIRM EACH STEP HAS RUN AFTER.   
    '''

    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df_ec = df.copy()
    print('✅ Power data acquired from link')

    #cache to local csv
    df_ec.to_csv('opsd.csv')
    print('✅ Power data cached to local csv')

    return df_ec

############################################################

def get_power():
    '''
THIS FUNCTION  RETURNS A DF OF THE OPEN POWER SYSTEMS DATA (OPSD) AND CHECKS TO SEE IF OPSD DATA EXISTS LOCALLY AS A 
CACHED CSV FILE. IF IT DOES IT READS THE FILE TO A DF. IF IT DOES NOT IT RUNS THE acquire_function() TO ACQUIRE THE 
DATA FROM THE LINK AND CACHES IT LOCALLY. THE PRINT STATEMENTS CONFIRM EACH STEP HAS RUN AFTER.    
    '''
    #if local file does not exist, acquire data from link provided
    if (os.path.isfile('opsd.csv') == False):
        print('Power data is not cached. Acquiring new data . . .')
        df = acquire_power()

    #if local csv file does exist, read to df
    else:
        print('Power data is cached. Reading to df...')
        df = pd.read_csv('opsd.csv')
        print('✅ Power data acquired from local csv file')

        #cache data to local csv
    
    df_ec = df.copy()

    return df_ec  

############################################################



############################################################



############################################################