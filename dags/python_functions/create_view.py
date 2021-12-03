from google.cloud import bigquery
import os

# Specify path for service account key.
credentials_path = '/opt/airflow/dags/python_functions/pythonbq.privateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def createView():

    client = bigquery.Client()

    view_id = "charming-aegis-332818.premier_league_2018_2019.man_united_2018_2019"
    source_id = "charming-aegis-332818.premier_league_2018_2019.results_2018_2019"
    view = bigquery.Table(view_id)

    # The source table in this example is created from a CSV file in Google
    view.view_query = f"SELECT * FROM `{source_id}` WHERE HomeTeam IN ('Man United') OR AwayTeam IN ('Man United')"

    # Make an API request to create the view.
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")