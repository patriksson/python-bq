from google.cloud import bigquery
import os

# Specify path for service account key.
credentials_path = '/opt/airflow/dags/python_functions/pythonbq.privateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def createTableBQ():

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "charming-aegis-332818.premier_league_2018_2019.results_2018_2019"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Date", "DATE", "NULLABLE"),
            bigquery.SchemaField("HomeTeam", "STRING", "NULLABLE"),
            bigquery.SchemaField("AwayTeam", "STRING", "NULLABLE"),
            bigquery.SchemaField("FTHG", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("FTAG", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("FTR", "STRING", "NULLABLE"),
            bigquery.SchemaField("HTHG", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("HTAG", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("HTR", "STRING", "NULLABLE"),
            bigquery.SchemaField("Referee", "STRING", "NULLABLE"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://my-case-bucket/season-1819.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))