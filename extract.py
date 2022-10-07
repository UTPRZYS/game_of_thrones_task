import requests
import json
import pandas as pd
from google.cloud import bigquery
import config

project_id = config.bigquery_params['project_id']
dataset_id = config.bigquery_params['dataset_id']

bigquery_client = bigquery.Client(project=project_id)
#dataset = bigquery_client.dataset("surf_task")

def get_table(p_table:str):
    pagesize = 30
    pagenumber=1

    urlform = r'https://www.anapioficeandfire.com/api/{p_table}?page={pagenumber}&pageSize={pagesize}'
    print(urlform.format(p_table=p_table, pagenumber=pagenumber, pagesize=pagesize))

    session = requests.Session()

    startnumber=pagenumber
    json_list = []
    while True:
        print(f'iteration start page {startnumber}')
        response = session.get(urlform.format(p_table=p_table, pagenumber=startnumber, pagesize=pagesize))
        print('Status code', response.status_code)
        batch_data_json = response.json()
        if not batch_data_json:
            print('breaking, no data')
            break
        else:
            print('still has data, continue')
            json_list.extend(batch_data_json)
            startnumber += 1
    return json_list

def process_table(p_table_name, p_json_table):
    df = pd.json_normalize(data = p_json_table)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    table_id=f"{project_id}.{dataset_id}.{p_table_name}"
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()
    table = bigquery_client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

if __name__ == '__main__':
    for elem in ['books','characters', 'houses']:
    #for elem in ['books']:
        json_table = get_table(elem)
        process_table(elem, json_table)

