with 

source as (

    select *,
        ST_GEOGPOINT(longitude, longitude) AS geo_point,
        EXTRACT(YEAR FROM CURRENT_DATE()) - birth_year as age 
    from {{ source('staging', 'user') }}

),

renamed as (

    select
        {{ dbt.safe_cast("id", api.Column.translate_type("integer")) }} as client_id,
        {{ dbt.safe_cast("age", api.Column.translate_type("integer")) }} as current_age,
        {{ dbt.safe_cast("retirement_age", api.Column.translate_type("integer")) }} as retirement_age,
        {{ dbt.safe_cast("birth_year", api.Column.translate_type("integer")) }} as client_birth_year,
        {{ dbt.safe_cast("birth_month", api.Column.translate_type("integer")) }} as client_birth_month,
        gender,
        address as client_address,

        cast(latitude as numeric) as latitude,
        cast(longitude as numeric) as longitude,
        city,
        state,
        'US' as country,

        cast(per_capita_income as numeric) as per_capita_income,
        cast(yearly_income as numeric) as yearly_income,
        case 
            when yearly_income >= per_capita_income then True 
            else False
        end as if_above_avg_income, 

        cast(total_debt as numeric) as total_debt,
        cast(credit_score as numeric) as credit_score,
        {{ dbt.safe_cast("num_credit_cards", api.Column.translate_type("integer")) }} as num_credit_cards
    from source

)

select * from renamed
