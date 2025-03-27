with 

source as (

    select 
        *,
        {{convert_yes_no_to_boolean('has_chip')}} as is_has_chip,
        {{convert_yes_no_to_boolean('card_on_dark_web')}} as is_card_on_dark_web
    from {{ source('staging', 'cards') }}

),

renamed as (

    select
        {{ dbt.safe_cast("id", api.Column.translate_type("integer")) }} as card_id,
        {{ dbt.safe_cast("client_id", api.Column.translate_type("integer")) }} as client_id,
        card_brand ,
        card_type,
        card_number,
        cast(expires as date) as card_expired_date,
        -- {{ dbt.safe_cast("cvv", api.Column.translate_type("integer")) }} as card_cvv,
        {{ dbt.safe_cast("is_has_chip", api.Column.translate_type("boolean")) }} as is_has_chip,
        num_cards_issued,
        cast(credit_limit as numeric) as credit_limit,
        cast(acct_open_date as date) as acct_open_date,
        {{ dbt.safe_cast("year_pin_last_changed", api.Column.translate_type("integer")) }} as year_pin_last_changed,
        {{ dbt.safe_cast("is_card_on_dark_web", api.Column.translate_type("boolean")) }} as is_card_on_dark_web
    from source

)

select * from renamed
