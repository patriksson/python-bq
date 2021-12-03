from google.cloud import bigquery
import os

# Specify path for service account key.
credentials_path = '/opt/airflow/dags/python_functions/pythonbq.privateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def createTempTable():

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the destination table.
    table_id = "charming-aegis-332818.premier_league_2018_2019.temp_results_table"

    job_config = bigquery.QueryJobConfig(destination=table_id)

    QUERY = """
            SELECT * FROM `charming-aegis-332818.premier_league_2018_2019.results_2018_2019`
            """

    # Start the query, passing in the extra configuration.
    query_job = client.query(QUERY, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))