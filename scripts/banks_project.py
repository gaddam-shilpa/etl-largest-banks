import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime



url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
Exchange_rate_CSV_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
Table_Attributes = "Name", "MC_USD_Billion"
Table_Attributes_final = "Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"
Output_CSV_Path = "./Largest_banks_data.csv"
Database_name = "Banks.db"
Table_name = "Largest_banks"
csv_path = "./exchange_rate.csv"

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = "%Y-%m-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp+ ":"+ message+ "\n")



def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, "html.parser")
    df = pd.DataFrame(columns = table_attribs)
    table = data.find_all("tbody")
    rows = table[0].find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols)!=0:
            if cols[1].find("a") is not None:
                data_dict = {"Name": cols[1].text.strip(),
                            "MC_USD_Billion":cols[2].text.strip()}
                df1 = pd.DataFrame([data_dict], index=[0])
                df = pd.concat([df,df1], ignore_index = True)

    return df



def transform(df, csv_path):
    ex = pd.read_csv(csv_path)
    gbp_rate = ex.loc[ex["Currency"] == "GBP", "Rate"].iloc[0]
    eur_rate = ex.loc[ex["Currency"] =="EUR", "Rate"].iloc[0]
    inr_rate = ex.loc[ex["Currency"] =="INR", "Rate"].iloc[0]
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype(float)
    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * gbp_rate,2)
    df["MC_EUR_Billion"] = round(df["MC_USD_Billion"] * eur_rate,2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * inr_rate,2)
    return df



def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)



def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    for q in query_statement:
        print(f"Running query: {q}\n")
        query_output = pd.read_sql(q, sql_connection)
        print(query_output,"\n")
    


log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url, Table_Attributes)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df,csv_path)

log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df, Output_CSV_Path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(Database_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection,Table_name )

log_progress('Data loaded to Database as a table, Executing queries')

queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) as avg_mc_gbp FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]

run_query(queries, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
log_progress('Server Connection closed')