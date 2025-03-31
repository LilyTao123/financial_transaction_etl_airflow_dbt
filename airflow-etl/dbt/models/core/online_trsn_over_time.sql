with
    trsn as (
        select
            transaction_id,
            {{ dbt.date_trunc("year", "transaction_date") }} as transaction_year,
            transaction_date,
            transaction_amount,
            client_id,
            card_id,
            merchant_id,
            merchant_country,
            merchant_state,
            merchant_city,
            is_online_transaction,
            transaction_status,
            fail_reason,
            b.code,
            b.descript
        from {{ ref("stg_trnsction") }} as a 
        left join {{ ref("stg_mcc") }} as b 
        on a.mcc = b.code 
        -- only successed transaction
        where transaction_status_code = 1
    )
select
    transaction_year, code, descript,
    sum(case when is_online_transaction then transaction_amount else 0 end) as online_transaction_amount,
        sum(transaction_amount) as total_transaction_amount,
    count(1) as cnt_of_transactions,
    count(case when is_online_transaction then 1 end) as cnt_of_online_transaction
from trsn
group by transaction_year, code, descript
