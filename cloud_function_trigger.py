# main.py
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


def load_to_bigquery(event, context):

    # Extract bucket and file information from the event
    bucket_name = event['bucket']
    file_name = event['name']

    # Specify the subdirectory and ensure file_name starts with it
    subdirectory = 'transformed/'
    if not file_name.startswith(subdirectory):
        print(f"Skipping file {file_name} as it's not in the '{subdirectory}' subdirectory.")
        return

    # Set up BigQuery client
    client = bigquery.Client()

    # Set up Storage client
    storage_client = storage.Client()

    # Get the bucket and file
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Load data into BigQuery
    dataset_id = 'top_500_dataset'
    table_id = 'top_500_players_new'
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    # Attempt to fetch the table; if not found, create a new one
    try:
        table = client.get_table(full_table_id)  # This verifies the table exists.
    except NotFound:
        # If the table does not exist, create a new table with autodetect.
        # Note: This creates a placeholder table, which will be properly schema'd upon first load.
        table_ref = client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref)
        table = client.create_table(table)  # API request
        print(f"Table {full_table_id} created since it did not exist.")

    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition='WRITE_APPEND',  # Overwrite the table
    )

    with blob.open("rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    load_job.result()  # Wait for the job to complete

    print(f"File {file_name} loaded to {dataset_id}.{table_id}")
