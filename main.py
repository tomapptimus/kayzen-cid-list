import requests
import base64
import json
import os
from google.cloud import bigquery
from datetime import datetime


def get_kayzen_access_token(api_key, api_secret, username, password):
    """Get access token from Kayzen API"""
    auth_string = f"{api_key}:{api_secret}"
    auth_header = base64.b64encode(auth_string.encode()).decode()

    headers_auth = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    body = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }

    response = requests.post(
        "https://api.kayzen.io/v1/authentication/token", 
        headers=headers_auth, 
        json=body
    )
    
    if response.status_code != 200:
        raise Exception(f"Authentication failed with status code {response.status_code}: {response.text}")
    
    return response.json()['access_token']


def fetch_all_campaigns(access_token):
    """Fetch all campaigns from Kayzen API with pagination"""
    all_campaigns = []
    page = 1
    per_page = 100

    while True:
        url = f"https://api.kayzen.io/v1/campaigns?page={page}&per_page={per_page}"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching data on page {page} with status code {response.status_code}: {response.text}")

        data = response.json().get("data", [])
        
        if not data:
            break

        all_campaigns.extend(data)
        page += 1

    return all_campaigns


def load_campaigns_to_bigquery(campaigns, project_id, dataset_id, table_id):
    """Load campaigns directly to BigQuery with upsert logic"""
    if not campaigns:
        print("No campaigns to load")
        return

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Create table if it doesn't exist
    try:
        table = client.get_table(table_ref)
    except Exception:
        # Table doesn't exist, create it with auto-detected schema and clustering
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            clustering_fields=["id"]  # Cluster by campaign ID for better performance
        )
        
        # Load first batch to create table
        load_job = client.load_table_from_json(
            campaigns[:1],  # Just one record to create schema
            table_ref,
            job_config=job_config
        )
        load_job.result()
        print(f"Created table {dataset_id}.{table_id} with clustering on 'id'")
    
    # Add fetch_timestamp to each campaign
    current_timestamp = datetime.utcnow().isoformat()
    for campaign in campaigns:
        campaign['fetch_timestamp'] = current_timestamp
    
    # Use MERGE statement for upsert logic
    # First, delete existing records for campaigns we're about to insert
    campaign_ids = [str(campaign.get('id', '')) for campaign in campaigns if campaign.get('id')]
    
    if campaign_ids:
        # Delete existing records for these campaign IDs
        delete_query = f"""
        DELETE FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE id IN UNNEST({campaign_ids})
        """
        
        delete_job = client.query(delete_query)
        delete_job.result()
        print(f"Deleted existing records for {len(campaign_ids)} campaign IDs")
    
    # Insert new records
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    load_job = client.load_table_from_json(
        campaigns,
        table_ref,
        job_config=job_config
    )
    
    load_job.result()
    print(f"Successfully loaded {len(campaigns)} campaigns to {dataset_id}.{table_id}")


def fetch_kayzen_campaigns(request):
    """Cloud Function entry point"""
    try:
        # Get credentials from environment variables
        api_key = os.environ.get('KAYZEN_API_KEY')
        api_secret = os.environ.get('KAYZEN_API_SECRET')
        username = os.environ.get('KAYZEN_USERNAME')
        password = os.environ.get('KAYZEN_PASSWORD')
        project_id = os.environ.get('GCP_PROJECT_ID')
        dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        table_id = os.environ.get('BIGQUERY_TABLE_ID')
        
        # Validate required environment variables
        required_vars = [api_key, api_secret, username, password, project_id, dataset_id, table_id]
        if not all(required_vars):
            raise ValueError("Missing required environment variables")
        
        print("Starting Kayzen campaigns fetch...")
        
        # Get access token
        access_token = get_kayzen_access_token(api_key, api_secret, username, password)
        print("✅ Access token obtained")
        
        # Fetch all campaigns
        campaigns = fetch_all_campaigns(access_token)
        print(f"✅ Fetched {len(campaigns)} campaigns")
        
        # Load to BigQuery
        load_campaigns_to_bigquery(campaigns, project_id, dataset_id, table_id)
        print("✅ Data loaded to BigQuery successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'campaigns_processed': len(campaigns)
            })
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
