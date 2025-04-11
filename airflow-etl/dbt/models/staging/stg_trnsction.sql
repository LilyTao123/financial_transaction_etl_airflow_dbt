with 

source as (

    select 
        tr.*,
        case 
            when errors = 'unknown' then 'success'
            else 'fail'
        end as transaction_status,
        case 
            when errors = 'unknown' then 1
            else 0
        end as transaction_status_code,
        case 
            when errors = 'unknown' then 'none'
            else errors 
        end as fail_reason,
        s.Country as country,
        s.States as state
    from {{ source('staging', 'trnsction') }} as tr
    left join {{ref("states_id")}} as s 
    on tr.merchant_state = s.ID 

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['transaction_time', 'client_id', 'card_id', 'amount', 'merchant_id']) }} as unique_transaction_id,
        {{ dbt.safe_cast("id", api.Column.translate_type("integer")) }} as transaction_id,
        transaction_time,
        EXTRACT(YEAR FROM transaction_time) as transaction_year,
        cast(transaction_time as date) as transaction_date,
        {{ dbt.safe_cast("client_id", api.Column.translate_type("integer")) }} as client_id,
        {{ dbt.safe_cast("card_id", api.Column.translate_type("integer")) }} as card_id,
        cast(amount as numeric) as transaction_amount,
        use_chip,
        {{ dbt.safe_cast("merchant_id", api.Column.translate_type("integer")) }} as merchant_id,
        merchant_state as raw_merchant_state,
        case 
            when lower(merchant_city) = 'online'
                then 'online'
            when country is null 
                then merchant_state 
            else country
        end as merchant_country,
        case 
            when lower(merchant_city) = 'online'
                then 'online'
            when state is null 
             then merchant_city
             else state
        end as merchant_state ,
        merchant_city,
        zip,
        {{ dbt.safe_cast("mcc", api.Column.translate_type("integer")) }}  as mcc,
        case 
            when lower(merchant_city) = 'online' then TRUE 
            else False 
        end as is_online_transaction,
        transaction_status,
        transaction_status_code,
        fail_reason

    from source
    -- where EXTRACT(YEAR FROM transaction_date) >= 2018
)

select * from renamed
