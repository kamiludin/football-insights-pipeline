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
    fe.goals,
    fe.xg,
    (CAST(fe.xg AS FLOAT) - fe.goals) as goal_diff
from
    dim_epl_players de
join
    fact_epl_players fe
on de.player_id = fe.player_id
where fe.xg > fe.goals
order by goal_diff desc
limit 5