# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3
from datetime import datetime
from pathlib import Path

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_Millions']
csv_path = Path(r'C:\Users\gclou\Documents\DataEngineering_Work\Python_Project_for_DE\3_ETL_GDP_Data\GDP_USD_Billions.csv')
conn = sqlite3.connect('Country_GDP.db')
table_name = 'GDP_USD_Billions'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    # Get all tables on the webpage
    tables = soup.find_all('tbody')

    # Get gdp table
    gdp_table = tables[2].find_all('tr')
    
    # Extract country and gdp
    for row in gdp_table:
        col = row.find_all('td')
        # Saftey check if there are colomns in the table
        if len(col) != 0:
            # if the country name has a link (because we want to exclude the first row of World GDP) 
            # and '-' is not in the GDP column (don't want missing values in the data)
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {
                    table_attribs[0]: col[0].a.contents[0],
                    table_attribs[1]: col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                
    

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    gdp_millions_text = df['GDP_USD_Millions']

    # join string on ,
    # example number 340,290,192
    # x.split(',') [340, 290, 192]
    # ''.join() 340290192
    # x.split will split the string at the specified delimeter ',' into a list 
    # ''.join will concatanate the list of strings with no delimiter unless specified
    gdp_millions_nums_text = gdp_millions_text.apply(lambda x: ''.join(x.split(',')))

    # type cast to float
    gdp_millions = gdp_millions_nums_text.apply(lambda x: float(x))

    # convert to billions
    gdp_billions = gdp_millions.apply(lambda x: round(x/1000), 2)

    df['GDP_USD_Millions'] = gdp_billions

    df.rename(columns={'GDP_USD_Millions': 'GDP_USD_Billions'}, inplace=True)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(f'Query Output:\n{query_output}')


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

    ''' Here, you define the required entities and call the relevant 
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


log_progress(f'Extracting data from {url}')
gdp_table = extract(url, table_attribs)
gdp_table

log_progress(f'Transforming GDP from Millions to Billions (USD)')
gdp_billions = transform(gdp_table)
gdp_billions

log_progress('Loading data to csv file \'GDP_USD_Billions\'')
load_to_csv(gdp_billions, csv_path)

log_progress('Loading data to database \'Country_GDP\'')
load_to_db(gdp_billions, conn, table_name)

query = f'SELECT * FROM {table_name}'
log_progress('Running query on newly created database and table for verification')
run_query(query, conn)