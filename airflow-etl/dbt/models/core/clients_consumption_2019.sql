 {{ config(materialized='table') }}

with trnsction as (
    select 
        transaction_id,
        {{ dbt.date_trunc("month", "transaction_date") }} as transaction_month, 
        transaction_date,
        transaction_amount,
        client_id,
        card_id, 
        mcc,
        merchant_id,
        merchant_country,
        merchant_state,
        merchant_city,
        is_online_transaction,
        transaction_status,
        fail_reason
    from {{ref("stg_trnsction")}}
    -- only successed transaction
    where transaction_status_code = 1
    and EXTRACT(YEAR FROM transaction_date) >= 2018
),
agg_trns as (
select 
    transaction_month, 
    client_id,
    merchant_country,
    merchant_state, 
    merchant_city,
    count(1) as cnt_of_transactions,
    sum(transaction_amount) as total_consumption,
    max(transaction_amount) as max_transaction_amount,
    count(CASE WHEN is_online_transaction THEN 1 END) as cnt_of_online_transaction
from trnsction
group by transaction_month, client_id, merchant_country, merchant_state, merchant_city
), 
max_trns as (
    -- Find the transaction with the max amount
    select 
        transaction_id,
        transaction_amount,
        code,
        descript,
        merchant_id,  -- Assuming this is your MCC field or adjust accordingly
        client_id,
        transaction_date
    from trnsction as a
    left join `careful-compass-455315-b1.financial_transaction_transformed_data.stg_mcc` b
    on a.mcc = b.code 
    where transaction_amount = (select max(transaction_amount) from trnsction)
)
select 
    agg_trns.*,
    max_trns.descript as max_trns_descrpt,
    users.country as users_country,
    users.state as users_state,
    users.city as usrs_city,
    users.gender,
    EXTRACT(YEAR FROM transaction_month) - users.client_birth_year as age 
from agg_trns
left join {{ref("stg_user")}} as users
on agg_trns.client_id = users.client_id
left join max_trns
on agg_trns.max_transaction_amount = max_trns.transaction_amount