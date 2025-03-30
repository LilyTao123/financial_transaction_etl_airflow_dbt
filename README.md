# Background
This project demonstrates a proof of concept for an end-to-end data ingestion pipeline, leveraging Terraform, Google Cloud Platform (GCP), Apache Airflow, and Docker. It processes financial transaction data from a banking institution and generates a comprehensive dashboard for analysis.
# Data source
This dataset is sourced from Kaggle, containing transaction data, user information, and card details from 2010 to 2019. To enhance its usability, I imported the dataset into my GitHub repository and re-mapped the state and country columns for better clarity and consistency.

# Pre-requisites
1. git
2. GCP
3. Terraform

# airflow
1. add connections dbt-cloud
Set the following:
Connection ID: dbt_cloud_conn
Connection Type: dbt Cloud
Account ID: Your dbt Cloud account ID.
Token: Your dbt Cloud API key (Get it from dbt Cloud → Account Settings → API Access).

2.spark
