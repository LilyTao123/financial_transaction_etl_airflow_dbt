from common.file_config import *
import os 

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("GCP_BIGQUERY_DATASET")


create_partitioned_transaction_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_trsnction} 
    PARTITION BY DATE(transaction_date) 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_trsnction};
""" 


create_user_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_user} 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_user};
""" 

create_cards_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_cards} 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_cards};
""" 

create_mcc_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_mcc} 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_mcc};
""" 

### new process

create_main_trnsction_table =  f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_trsnction} (
                        unique_id STRING,
                        id INTEGER,
                        transaction_time TIMESTAMP,
                        client_id INTEGER,
                        card_id INTEGER,
                        amount NUMERIC,
                        use_chip STRING,
                        merchant_id INTEGER,
                        merchant_city STRING,
                        merchant_state STRING,
                        zip STRING,
                        mcc INTEGER,
                        errors STRING
                        )
    PARTITION BY DATE(transaction_time)
""" 

create_stg_transaction_table_query = f""" 
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{bq_trsnction}_stg` AS
        WITH base_data AS (
            SELECT
                -- Use TO_HEX to convert MD5 bytes to string type
                 TO_HEX(MD5(CONCAT(
                     COALESCE(cast(transaction_time as string), ""), '|',
                     COALESCE(cast(client_id as string), ""), '|',
                     COALESCE(cast(card_id as string), ""), '|',
                     COALESCE(cast(amount as string), ""), '|',
                     COALESCE(cast(merchant_id as string), ""), '|',
                     COALESCE(cast(mcc as string), ""), '|',
                     COALESCE(use_chip, ""), '|'
                 ))) AS row_md5sum,
                        id,
                        transaction_time,
                        client_id,
                        card_id,
                        cast(amount as numeric) as amount,
                        use_chip,
                        merchant_id,
                        merchant_city,
                        merchant_state,
                        zip,
                        cast(mcc as integer) as mcc,
                        errors
             FROM `{PROJECT_ID}.{DATASET_ID}.{bq_external_trsnction}`
         ),         
         unique_rows AS (
            SELECT
                *,
                ROW_NUMBER() OVER(
                    PARTITION BY row_md5sum 
                    ORDER BY transaction_time, client_id, card_id, merchant_id, amount
                ) AS row_counter,
            FROM base_data
        )
        SELECT
            -- Create a unique identifier by concatenating hashsum and counter
            CONCAT(row_md5sum, '-', 
                ROW_NUMBER() OVER(PARTITION BY row_md5sum ORDER BY transaction_time, client_id, card_id, merchant_id, amount)
            ) AS unique_id,
        id,
        transaction_time,
        client_id,
        card_id,
        amount,
        use_chip,
        merchant_id,
        merchant_city,
        merchant_state,
        zip,
        mcc,
        errors
        FROM unique_rows;

""" 

merge_partitioned_transaction_table_query = f""" 
 MERGE INTO `{PROJECT_ID}.{DATASET_ID}.{bq_trsnction}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.{bq_trsnction}_stg` S
                    ON T.unique_id = S.unique_id
                    
                    -- Only when not matched, insert the new row
                    WHEN NOT MATCHED THEN
                        INSERT (
                            id,
                            transaction_time,
                            client_id,
                            card_id,
                            amount,
                            use_chip,
                            merchant_id,
                            merchant_city,
                            merchant_state,
                            zip,
                            mcc,
                            errors
                        )                     
                        VALUES (
                            S.unique_id,
                            S.id,
                            S.transaction_time,
                            S.client_id,
                            S.card_id,
                            S.amount,
                            S.use_chip,
                            S.merchant_id,
                            S.merchant_city,
                            S.merchant_state,
                            S.zip,
                            S.mcc,
                            S.errors
                        );
""" 


create_main_user_table =  f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_trsnction} (
                        unique_id STRING,
                        id INTEGER,
                        client_id INTEGER,
                        card_id INTEGER,
                        amount NUMERIC,
                        use_chip STRING,
                        merchant_id INTEGER,
                        merchant_city STRING,
                        merchant_state STRING,
                        zip STRING,
                        mcc INTEGER,
                        errors STRING,
                        transaction_time TIMESTAMP
                        )
    PARTITION BY DATE(transaction_time)
""" 

create_main_cards_table =  f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_trsnction} (
                        unique_id STRING,
                        id INTEGER,
                        client_id INTEGER,
                        card_id INTEGER,
                        amount NUMERIC,
                        use_chip STRING,
                        merchant_id INTEGER,
                        merchant_city STRING,
                        merchant_state STRING,
                        zip STRING,
                        mcc INTEGER,
                        errors STRING,
                        transaction_time TIMESTAMP
                        )
    PARTITION BY DATE(transaction_time)
""" 