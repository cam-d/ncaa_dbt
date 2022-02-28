from google.cloud import bigquery
import os
import pandas as pd
var_string = #path to bigquery credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = var_string

def load_table_file(file_path, table_id):

    # [START bigquery_load_from_file]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    # [END bigquery_load_from_file]
    return table

folder_path = #path to folder with kaggle files

os.chdir(folder_path)

for root, dirs, files in os.walk(".", topdown=False):
    for name in files:
        file_path = folder_path+'\\'+name
        table_name = name.lower().split('.')[0]
        bq_project = #gcp projectid or bq dataset
        bq_schema = #bq schema
        table_id = bq_project+bq_schema+table_name

        load_table_file(file_path, table_id )