from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
from collections import defaultdict
import pandas as pd
import glob
import argparse
import random


parser = argparse.ArgumentParser(description='Check the deletion status of table for projects')
parser.add_argument('-n', '--name', help="project name", type=str, required = True)
parser.add_argument('-c', '--csv', help="csv name", type=str, default="fivetran_remove_list - 시트2")
args = parser.parse_args()

project_name = args.name
csv_name = args.csv


key_path = glob.glob(f"../admin/{project_name}-*.json")[0]
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials = credentials, project = credentials.project_id)


##Check datasets name 
datasets = list(client.list_datasets())  # Make an API request.
project = client.project
datasets_name = set()

if datasets:
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        datasets_name.add(dataset.dataset_id)
        print("\t{}".format(dataset.dataset_id))
else:
    print("{} project does not contain any datasets.".format(project))
    quit()


df_csv = pd.read_csv(f'../csv/{csv_name}.csv')

def sql_tb(tb_name, sc_name):
    return f"""SELECT IFNULL( (SELECT table_name 
                              FROM {project_name}.{sc_name}.INFORMATION_SCHEMA.TABLES 
                              WHERE table_name = '{tb_name}'),'Y') as result"""
  

def sql_col(tb_name, sc_name, col_name):
    return f"""SELECT IFNULL((SELECT column_name
              FROM {project_name}.{sc_name}.INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_NAME = '{tb_name}' 
                    AND COLUMN_NAME = '{col_name}'),"Y") as result"""



def checkDeleted(df_csv, col_sc ="schema", col_tb ="table", col_c="column"):
    for idx, row in df_csv.iterrows():
        cur_schema = row[col_sc]
        if (cur_schema not in datasets_name):
            continue
        cur_tb = row[col_tb]
        cur_c = row[col_c]
        cur_sql = sql_tb(cur_tb,cur_schema)
        cur_df = bq2df(cur_sql)

        if ( cur_df["result"].iloc[0] =="Y" ):
            df_csv["remove"].iloc[idx] = "Y"
        elif ( not pd.isnull(row[col_c]) ):
            col_sql = sql_col(cur_tb, cur_schema, cur_c)
            col_df = bq2df(col_sql)

            if ( col_df["result"].iloc[0] == "Y" ):
                df_csv["remove"].iloc[idx] = "Y"
            else:
                df_csv["remove"].iloc[idx] = "N"
        else:
            df_csv["remove"].iloc[idx] = "N"

def bq2df(sql):
    query_job = client.query(sql)
    df = query_job.to_dataframe()
    return df

def generateRand():
    return random.randint(10**12, 10**13 - 1)

def checkDatahub(idx_arr,df_csv):
    for idx, row in df_csv.iterrows():
        if (idx not in idx_arr):
            df_csv["datahub"].iloc[idx] = "Y"
        else:
            df_csv["datahub"].iloc[idx] = "N"


rand_num = generateRand()
checkDeleted(df_csv)    
found_idx = [30,32,82,83,86,87]
checkDatahub(found_idx,df_csv)
 
df_csv.to_csv (f'../csv/result_{project_name}_{rand_num}.csv', index = None, header=True) 
    
