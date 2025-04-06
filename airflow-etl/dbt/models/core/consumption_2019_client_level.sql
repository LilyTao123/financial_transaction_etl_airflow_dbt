with trns as (
select 
    transaction_year,
    client_id,
    sum(transaction_amount) as total_amount,
    count(1) as cnt_of_trnsction
from {{ref("stg_trnsction")}} as trns 
group by transaction_year, client_id
)
select
    trns.*,
    case 
        when trns.total_amount > user.yearly_income then 1 else 0
    end as comsumption_over_income,
    user.per_capita_income,
    user.yearly_income,
    user.country as users_country,
    user.state as users_state,
    user.city as usrs_city
from trns
left join {{ref("stg_user")}} as user
on trns.client_id = user.client_id