from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from python_functions.recreate_org_table import recreateOrgTable
from python_functions.create_temp_table import createTempTable
from python_functions.create_table_from_gcs import createTableBQ
from python_functions.create_view import createView
from python_functions.add_column import addColumn
from python_functions.delete_org_table import deleteOrgTable
from python_functions.recreate_org_table import recreateOrgTable
from python_functions.delete_copy_org_table import deleteCopyOfOrgTable
from python_functions.delete_view import deleteView


default_dag_args = {
    'owner': 'Andreas Patriksson',
    'use_legacy_sql': False,
    'start_date': days_ago(1)
}

with models.DAG(
    dag_id = 'case_workflow',
    default_args=default_dag_args,
    schedule_interval='@once'
) as dag:
    create_bq_table = PythonOperator(
        task_id='create_bq_table_from_gcs',
        python_callable=createTableBQ
    )

    create_view = PythonOperator(
            task_id='create_view',
            python_callable=createView,
            depends_on_past=True
    )

    add_column = PythonOperator(
            task_id='add_column',
            python_callable=addColumn,
            depends_on_past=True
    )

    create_copy_of_org_table = PythonOperator(
            task_id='create_copy_of_org_table',
            python_callable=createTempTable,
            depends_on_past=True
    )

    delete_org_table = PythonOperator(
            task_id='delete_org_table',
            python_callable=deleteOrgTable,
            depends_on_past=True
    )

    recreate_org_table = PythonOperator(
            task_id='recreate_org_table',
            python_callable=recreateOrgTable,
            depends_on_past=True
    )

    delete_copy_of_org_table = PythonOperator(
            task_id='delete_copy_of_org_table',
            python_callable=deleteCopyOfOrgTable,
            depends_on_past=True
    )

    delete_view = PythonOperator(
            task_id='delete_view',
            python_callable=deleteView,
            depends_on_past=True
    )

    create_bq_table >> create_view >> add_column >> create_copy_of_org_table >> delete_org_table >> recreate_org_table >> delete_copy_of_org_table >> delete_view