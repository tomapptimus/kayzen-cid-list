# Kayzen Campaigns Fetcher

A Google Cloud Function that fetches campaign data from the Kayzen API and stores it in BigQuery with intelligent upsert logic.

## Features

- 🔐 Secure credential management using environment variables
- 📊 Direct BigQuery loading (no intermediate storage)
- 🔄 Smart upsert logic - overwrites existing campaigns with latest data
- ☁️ Cloud Functions deployment via Cloud Build
- 📈 Automatic pagination to fetch all available campaigns
- 🛡️ Production-ready error handling and logging

## How It Works

1. **Authentication**: Gets access token from Kayzen API using Basic Auth
2. **Data Fetching**: Retrieves all campaigns with automatic pagination
3. **Upsert Logic**: 
   - Deletes existing records for campaigns being updated
   - Inserts new/updated campaign data
   - Preserves campaigns not found in current fetch
4. **Storage**: Loads data directly into BigQuery table

## Setup

### 1. Prerequisites

- Google Cloud Project with BigQuery enabled
- Kayzen API credentials
- Cloud Build API enabled
- Cloud Functions API enabled

### 2. Environment Variables

Copy `env.example` to `.env` and fill in your credentials:

```bash
cp env.example .env
```

Required variables:
- `KAYZEN_API_KEY`: Your Kayzen API key
- `KAYZEN_API_SECRET`: Your Kayzen API secret
- `KAYZEN_USERNAME`: Your Kayzen username
- `KAYZEN_PASSWORD`: Your Kayzen password
- `GCP_PROJECT_ID`: Your Google Cloud Project ID
- `BIGQUERY_DATASET_ID`: BigQuery dataset name
- `BIGQUERY_TABLE_ID`: BigQuery table name

### 3. Deploy to Google Cloud

#### Option A: Using Cloud Build (Recommended)

1. Create a Cloud Build trigger in your Google Cloud Console
2. Connect to your repository
3. Set the following substitution variables in the trigger:
   - `_KAYZEN_API_KEY`
   - `_KAYZEN_API_SECRET`
   - `_KAYZEN_USERNAME`
   - `_KAYZEN_PASSWORD`
   - `_BIGQUERY_DATASET_ID`
   - `_BIGQUERY_TABLE_ID`

4. Run the trigger to deploy

#### Option B: Manual Deployment

```bash
# Deploy to Cloud Functions (2nd gen)
gcloud functions deploy kayzen-campaigns-fetcher \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --memory 512MB \
  --timeout 300s \
  --region europe-west1 \
  --gen2 \
  --source . \
  --entry-point fetch_kayzen_campaigns \
  --set-env-vars KAYZEN_API_KEY=your-key,KAYZEN_API_SECRET=your-secret,...
```

### 4. Schedule Daily Runs

Set up a Cloud Scheduler job to trigger the function daily:

```bash
gcloud scheduler jobs create http kayzen-campaigns-daily \
  --schedule="0 9 * * *" \
  --uri="https://europe-west1-PROJECT_ID.cloudfunctions.net/kayzen-campaigns-fetcher" \
  --http-method=GET
```

## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export KAYZEN_API_KEY=your-key
export KAYZEN_API_SECRET=your-secret
# ... etc
```

3. Run locally:
```bash
# For local testing, you can use the Google Cloud Functions Framework
pip install functions-framework
functions-framework --target=fetch_kayzen_campaigns --port=8080
```

## BigQuery Schema

The function automatically creates the table with schema detected from the Kayzen API response. Each record includes:

- All fields from the Kayzen API response
- `fetch_timestamp`: ISO timestamp when the data was fetched
- `id`: Campaign ID (used for upsert logic)

### Performance Optimizations

- **Clustering**: Table is clustered by `id` for optimal query performance
- **Auto-schema**: Schema is automatically detected from Kayzen API response
- **Upsert logic**: Efficiently updates existing campaigns and adds new ones

## Security

- All sensitive data is stored in environment variables
- No credentials are committed to the repository
- Uses Google Cloud IAM for BigQuery access
- Function runs with minimal permissions

## Monitoring

- Check Cloud Functions logs for execution details
- Monitor BigQuery table for data freshness
- Set up alerts for function failures

## License

MIT License - feel free to use and modify as needed.
