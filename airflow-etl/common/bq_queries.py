from common.file_config import *
import os 

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("GCP_BIGQUERY_DATASET")

# transaction

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
                            unique_id,
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
# user 

create_main_user_table =  f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_user} (
                        id INTEGER,
                        current_age INTEGER,
                        retirement_age INTEGER,
                        birth_year INTEGER,
                        birth_month INTEGER,
                        gender STRING,
                        address STRING,
                        latitude FLOAT64,
                        longitude FLOAT64,
                        per_capita_income FLOAT64,
                        yearly_income FLOAT64,
                        total_debt FLOAT64,
                        credit_score INTEGER,
                        num_credit_cards INTEGER,
                        city STRING,
                        state STRING
                        )
"""

merge_user_table_query = f"""  
    MERGE INTO `{PROJECT_ID}.{DATASET_ID}.{bq_user}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.external_user` S
                    ON T.id = S.id
                    
                    -- Only when not matched, insert the new row
                    WHEN NOT MATCHED THEN
                        INSERT (
                            id ,
                            current_age ,
                            retirement_age ,
                            birth_year ,
                            birth_month ,
                            gender ,
                            address ,
                            latitude ,
                            longitude ,
                            per_capita_income ,
                            yearly_income ,
                            total_debt ,
                            credit_score ,
                            num_credit_cards ,
                            city ,
                            state  
                        )                     
                        VALUES (
                            S.id ,
                            S.current_age ,
                            S.retirement_age ,
                            S.birth_year ,
                            S.birth_month ,
                            S.gender ,
                            S.address ,
                            S.latitude ,
                            S.longitude ,
                            S.per_capita_income ,
                            S.yearly_income ,
                            S.total_debt ,
                            S.credit_score ,
                            S.num_credit_cards ,
                            S.city ,
                            S.state  
                        );

"""

# cards

create_main_cards_table =  f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_cards} (
                        id INTEGER,
                        client_id INTEGER,
                        card_brand STRING,
                        card_type STRING,
                        card_number STRING,
                        expires STRING,
                        cvv INTEGER,
                        has_chip STRING,
                        num_cards_issued INTEGER,
                        credit_limit FLOAT64,
                        acct_open_date STRING,
                        year_pin_last_changed INTEGER,
                        card_on_dark_web STRING
                        )
""" 

merge_cards_table_query =  f""" 
    MERGE INTO `{PROJECT_ID}.{DATASET_ID}.{bq_cards}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.external_cards` S
                    ON T.id = S.id
                    
                    -- Only when not matched, insert the new row
                    WHEN NOT MATCHED THEN
                        INSERT (
                            id ,
                            client_id ,  
                            card_brand ,
                            card_type ,
                            card_number ,
                            expires ,
                            cvv ,
                            has_chip ,
                            num_cards_issued ,
                            credit_limit ,
                            acct_open_date ,
                            year_pin_last_changed ,
                            card_on_dark_web 
                        )                     
                        VALUES (
                            S.id ,
                            S.client_id ,  
                            S.card_brand ,
                            S.card_type ,
                            S.card_number ,
                            S.expires ,
                            S.cvv ,
                            S.has_chip ,
                            S.num_cards_issued ,
                            S.credit_limit ,
                            S.acct_open_date ,
                            S.year_pin_last_changed ,
                            S.card_on_dark_web 
                        );
""" 

# mcc

create_main_mcc_table =  f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_mcc} (
                        code INTEGER,
                        descript STRING
                        )
""" 

merge_to_mcc_table = f""" 
    MERGE INTO `{PROJECT_ID}.{DATASET_ID}.{bq_mcc}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.external_mcc` S
                    ON T.code = S.code
                    
                    -- Only when not matched, insert the new row
                    WHEN NOT MATCHED THEN
                        INSERT (
                            code,
                            descript
                        )                     
                        VALUES (
                            S.code ,
                            S.descript 
                        );

""" 


#################


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

