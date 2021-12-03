from google.cloud import bigquery
import os

# Specify path for service account key.
credentials_path = '/opt/airflow/dags/python_functions/pythonbq.privateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def deleteOrgTable():

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to fetch.
    table_id = 'charming-aegis-332818.premier_league_2018_2019.results_2018_2019'

    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))