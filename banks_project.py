import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

#Global Scope
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billions"]
csv_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_Banks"
conn = sqlite3.connect(db_name)

#Log
def log_progress(message):
    timestamp_format = "%Y-%m-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + " : " + message + '\n')

def extract(url, table_attribs):
    response = requests.get(url).text
    soup = BeautifulSoup(response,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('table', {'class':'wikitable'})
    body = tables[2].find('tbody')
    rows = body.find_all('tr')
    for row in rows[2:12]:
        cols = row.find_all('td')
        if len(cols) > 0:
            anchor = cols[0].find_all('a')
            name = anchor[1].text.strip()
            bank_name = name
            market_cap = cols[2].text.strip().replace(",","")
            if market_cap:
                market_cap = float(market_cap)
            else:
                market_cap = np.nan
            data_dict = {"Name" : bank_name,
                         "MC_USD_Billions" : market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    exchange_rate = pd.read_csv('exchange_rate.csv')
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'], 2) for x in df["MC_USD_Billions"]]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'], 2) for x in df["MC_USD_Billions"]]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'], 2) for x in df["MC_USD_Billions"]]
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, connection, table_name):
    df.to_sql(table_name, connection, if_exists='replace', index=False)

def run_queries(query_statement, connection):
    query_output = pd.read_sql(query_statement, connection)
    return query_output

#Test

log_progress("Initiate ETL Process")
df = extract(url, table_attribs)

log_progress("Extract phase Complete")
df = transform(df)

log_progress("Transform phase Complete")
load_to_csv(df, csv_path)
load_to_db(df, conn, table_name)

log_progress("Load phase Complete")

query_statement = f"SELECT * FROM {table_name}"

print(run_queries(query_statement, conn))










