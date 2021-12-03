from google.cloud import bigquery
import os

# Specify path for service account key.
credentials_path = '/opt/airflow/dags/python_functions/pythonbq.privateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def addColumn():

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table
    #                  to add an empty column.
    table_id = 'charming-aegis-332818.premier_league_2018_2019.results_2018_2019'

    table = client.get_table(table_id)  # Make an API request.

    original_schema = table.schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    new_schema.append(bigquery.SchemaField("Venue", "STRING", "NULLABLE"))

    table.schema = new_schema
    table = client.update_table(table, ["schema"])  # Make an API request.

    if len(table.schema) == len(original_schema) + 1 == len(new_schema):
        print("A new column has been added.")
    else:
        print("The column has not been added.")