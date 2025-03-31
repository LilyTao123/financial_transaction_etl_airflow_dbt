# Background
This project demonstrates a proof of concept for an end-to-end data ingestion pipeline, leveraging Terraform, Google Cloud Platform (GCP), Apache Airflow, and Docker. It processes financial transaction data from a banking institution, generating a comprehensive dashboard that analyzes customer behavior by comparing 2019 with 2018. It also includes trends in customers' online consumption over time.  
For accuracy, only successful transactions were considered in the analysis, as the dataset also contains fraudulent transactions.

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
``` git clone https://github.com/LilyTao123/financial_transaction_etl_airflow_dbt.git ```
## Create GCP service account and credential keys
### a. create service account
create a service account: IAM and admin -> service accounts -> click 'service account', and include the below permissions
```
  - BigQuery Admin
  - Compute Admin
  - Project IAM Admin
  - Service Account Admin
  - Service Account User
  - Storage Admin
``` 
### b. create credential keys
create credential key under the service account, click 'JSON', and download it, rename as 'google_creds.json'

### c. rename and save it
save and rename the keys as under airflow-etl, the path of it should be 'airflow-etl/.keys/google_cloud/google_creds.json'

## Update .env
```  
GCP_PROJECT_ID=<your project id>
GCP_GCS_BUCKET=<the bucket you want to create>
GCP_BIGQUERY_DATASET=<the dataset you want to create>
AIRFLOW_UID=1001
```

## copy .env file into airflow-etl

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
docker-compose build
docker-compose up
```

## Access to airflow
open ```http://localhost:8080 ``` in your browser
### Add spark connection
In the top navigation bar, go to Admin > Connections > + > Fill in the following: Connection Id: 'spark-conn' Connection Type: 'spark' Host: 'spark://spark-master' Port: '7077'
### Run dags as follow orders
dimension_ingestion_gcs_dag >> trnsaction_ingestion_gcs_dag >> dbt_run_job
After it runs successfully, you will observe the following:
1. New GCS bucket named <your-project-id>-finance-:  
  user_data.parquet  
  cards_data.parquet  
  transaction_data.parquet  
2. New tables in your GCS BigQuery dataset financial_transaction:
  user
  cards    
  trnsction
3. New tables in your GCS BigQuery dataset financial_transaction:
  clients_consumption_2019  
  online_trsn_over_time  
## Destroy resources
``` terraform destroy ```

# Data Visualisation

