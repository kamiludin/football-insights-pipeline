with dim_epl_players as (
    select * from {{ ref('dim_epl_players') }}
),

fact_epl_players as (
    select * from {{ ref('fact_epl_players') }}
)

select
    de.player_id,
    de.player_name,
    de.team_title,
    fe.goals
from
    dim_epl_players de
join
    fact_epl_players fe
on de.player_id = fe.player_id
order by goals desc
limit 10