{{ config(materialized='table') }}


with users as (
    select 
        client_id ,
        gender,
        if_above_avg_income,
        state,
        city,
        country,
        yearly_income,
        total_debt,
        credit_score
    from {{ref("stg_user")}}
),
cards as (
    select 
        card_id,
        card_type,
        card_expired_date,
        acct_open_date,
        credit_limit
    from {{ref('stg_cards')}}
),
trsn as (
    select 
        transaction_date,
        transaction_amount,
        merchant_country,
        merchant_state,
        merchant_city,
        is_online_transaction,
        transaction_status,

        client_id,
        card_id,
    from {{ref('stg_trnsction')}}
)
select 
    trsn.*,
    users.gender,
    users.if_above_avg_income,
    users.state,
    users.city,
    users.country,
    users.yearly_income,
    users.total_debt,
    users.credit_score,
    cards.card_type,
    cards.card_expired_date,
    cards.acct_open_date,
    cards.credit_limit
from trsn 
left join users
on trsn.client_id = users.client_id
left join cards 
on trsn.card_id = cards.card_id