# Background
This project demonstrates a proof of concept for an end-to-end data ingestion pipeline, leveraging Terraform, Google Cloud Platform (GCP), Apache Airflow, and Docker. It processes financial transaction data from a banking institution and generates a comprehensive dashboard for analysis.

# Data source
This dataset is sourced from Kaggle, containing transaction data, user information, and card details from 2010 to 2019. To enhance its usability, I imported the dataset into my GitHub repository and re-mapped the state and country columns for better clarity and consistency.

# Technology

# ETL Process Design
Airflow is used to initally ingest and transform data, the ingested data include:
1. transaction_data
2. user_data
3. cards_data

DBT transform data.

# Dats warehouse
All processed data is stored in a Google Cloud Storage (GCS) bucket and loaded into BigQuery.

# Pre-requisites
1. git
2. GCP
3. Terraform

# Get started
## clone git repository
## Create GCP service account and credential keys
### a. service account
create a service account: IAM and admin -> service accounts -> click 'service account', and include the below permissions
```
  - BigQuery Admin
  - Compute Admin
  - Project IAM Admin
  - Service Account Admin
  - Service Account User
  - Storage Admin
``` 
### b. credential keys
create credential key under the service account

### c. rename and save it
save and rename the keys as 'airflow-etl/.keys/google_cloud/google_creds.json'

## Update .env
```  
GCP_PROJECT_ID=<your project id>
GCP_GCS_BUCKET=<the bucket you want to create>
GCP_BIGQUERY_DATASET=<the dataset you want to create>
```

## Set up Terraform
1. initialise terraform
   ``` terraform init ```
2. preview the plan
   ``` terraform plan ```
3. create resources
   ``` terraform apply```

## Build airflow image
copy .env into airflow-etl
``` 
cd airflow-etl
docker-compose build ```
``` docker-compose up ```

## Access to airflow

