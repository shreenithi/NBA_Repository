from nba_api.stats.endpoints import leagueleaders
from google.cloud import storage
import pandas as pd
from datetime import datetime
import os

# Initialize Google Cloud Storage Client
storage_client = storage.Client()


def upload_blob(bucket_name, destination_blob_name, source_file_path):
    """Uploads a file to the specified bucket."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path, content_type='text/csv')
    print(f"File {source_file_path} uploaded to {bucket_name} as {destination_blob_name}")


def fetch_and_upload_league_leaders(bucket_name, file_name_prefix):
    """Fetches league leaders and uploads the CSV to 'raw/' directory."""
    # Fetch league leaders data
    league_leaders_df = leagueleaders.LeagueLeaders(
        season='2023-24',
        season_type_all_star='Regular Season',
        stat_category_abbreviation='PTS'
    ).get_data_frames()[0][:500]

    # Local path for temporary storage
    local_file_path = f"/tmp/{file_name_prefix}_data.csv"
    league_leaders_df.to_csv(local_file_path, index=False)

    # Upload to GCS
    upload_blob(bucket_name, f'raw/{file_name_prefix}_data.csv', local_file_path)

    # Cleanup local file
    os.remove(local_file_path)


def transform_data(bucket_name, source_blob_name):
    """Transforms data by adding a timestamp and uploads it to 'transformed/'."""
    source_blob = storage_client.bucket(bucket_name).blob(source_blob_name)
    destination_blob_name = source_blob_name.replace('raw/', 'transformed/')

    # Download the source blob to a temporary local file
    temp_local_filename = '/tmp/temp_transformed_file.csv'
    source_blob.download_to_filename(temp_local_filename)

    # Read, transform, and write the data
    data = pd.read_csv(temp_local_filename)
    data['timestamp'] = datetime.now().strftime('%Y-%m-%d')
    data.to_csv(temp_local_filename, index=False)

    # Upload the transformed file
    upload_blob(bucket_name, destination_blob_name, temp_local_filename)

    # Cleanup local file
    os.remove(temp_local_filename)
