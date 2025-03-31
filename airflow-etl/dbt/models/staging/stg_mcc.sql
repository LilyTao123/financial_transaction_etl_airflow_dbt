with 

source as (

    select 
        *
    from {{ source('staging', 'mcc') }}

),

renamed as (

    select
        {{ dbt.safe_cast("code", api.Column.translate_type("integer")) }} as code,
        descript
    from source

)

select * from renamed
