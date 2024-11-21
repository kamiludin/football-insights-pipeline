with dim_laliga_players as (
    select * from {{ ref('dim_laliga_players') }}
),

fact_laliga_players as (
    select * from {{ ref('fact_laliga_players') }}
)

select
    de.player_id,
    de.player_name,
    de.team_title,
    fe.goals
from
    dim_laliga_players de
join
    fact_laliga_players fe
on de.player_id = fe.player_id
order by goals desc
limit 10