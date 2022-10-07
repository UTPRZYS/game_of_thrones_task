from google.cloud import bigquery
from typing import List
import config

# Get parameters of BigQuery project and dataset
# Dataset and project must exists in BigQuery
project_id = config.bigquery_params['project_id']
dataset_id = config.bigquery_params['dataset_id']
fully_qualified = f'{project_id}.{dataset_id}'

# Initialize BigQuery client
bigquery_client = bigquery.Client(project=project_id)

def create_characters_schema():
    """Create schema for characters table"""

    schema = [
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("culture", "STRING"),
        bigquery.SchemaField("born", "STRING"),
        bigquery.SchemaField("died", "STRING"),
        bigquery.SchemaField("titles", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("title", "STRING")]
                             )
        ),
        bigquery.SchemaField("aliases", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("alias", "STRING")]
                             )
        ),
        bigquery.SchemaField("father", "STRING"),
        bigquery.SchemaField("mother", "STRING"),
        bigquery.SchemaField("spouse", "STRING"),
        bigquery.SchemaField("allegiances", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("allegiance", "STRING")]
                             )
        ),
        bigquery.SchemaField("books", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("book", "STRING")]
                             )
        ),
        bigquery.SchemaField("povBooks", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("povBook", "STRING")]
                             )
        ),
        bigquery.SchemaField("tvSeries", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("tvSerie", "STRING")]
                             )
        ),
        bigquery.SchemaField("playedBy", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("actor", "STRING")]
                             )
        )
    ]
    return schema

def create_books_schema():
    """Create schema for books table"""
    schema = [
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("isbn", "STRING"),
        bigquery.SchemaField("authors", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("author", "STRING")]
                             )
        ),
        bigquery.SchemaField("numberOfPages", "INTEGER"),
        bigquery.SchemaField("publisher", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("mediaType", "STRING"),
        bigquery.SchemaField("released", "DATETIME"),
        bigquery.SchemaField("characters", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("character", "STRING")]
                             )
        ),
        bigquery.SchemaField("povCharacters", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("povCharacter", "STRING")]
                             )
        )
    ]
    return schema

def create_houses_schema():
    """Create schema for houses table"""

    schema = [
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("coatOfArms", "STRING"),
        bigquery.SchemaField("words", "STRING"),
        bigquery.SchemaField("titles", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("title", "STRING")]
                             )
        ),
        bigquery.SchemaField("seats", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("seat", "STRING")]
                             )
        ),
        bigquery.SchemaField("currentLord", "STRING"),
        bigquery.SchemaField("heir", "STRING"),
        bigquery.SchemaField("overlord", "STRING"),
        bigquery.SchemaField("founded", "STRING"),
        bigquery.SchemaField("founder", "STRING"),
        bigquery.SchemaField("diedOut", "STRING"),
        bigquery.SchemaField("ancestralWeapons", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("ancestralWeapon", "STRING")]
                             )
        ),
        bigquery.SchemaField("cadetBranches", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("cadetBranch", "STRING")]
                             )
        ),
        bigquery.SchemaField("swornMembers", "RECORD", mode='REPEATED',
                             fields=(
                                 [bigquery.SchemaField("swornMember", "STRING")]
                             )
        )
    ]
    return schema

def create_table(p_table_name:str, p_schema:List):
    """Creates table p_table_name in BigQuery, if exists truncate the table"""

    print(f'Start processing table: {p_table_name}')
    table_id=f"{project_id}.{dataset_id}.{p_table_name}"
    table = bigquery.Table(table_id, schema=p_schema)
    bigquery_client.delete_table(table, not_found_ok = True)
    table = bigquery_client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

if __name__ == "__main__":
    tables = [
        {'tablename': 'characters', 'funct_name': create_characters_schema},
        {'tablename': 'books', 'funct_name': create_books_schema},
        {'tablename': 'houses', 'funct_name': create_houses_schema},
    ]
    for table in tables:
        schema = table['funct_name']()
        create_table(table['tablename'], schema)

