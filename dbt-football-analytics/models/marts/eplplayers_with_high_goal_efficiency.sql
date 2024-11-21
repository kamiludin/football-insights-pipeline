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
    fe.shots,
    (CAST(fe.goals AS FLOAT) / fe.shots) as goal_efficiency
from
    dim_epl_players de
join
    fact_epl_players fe
on de.player_id = fe.player_id
where fe.goals > 15
order by goal_efficiency desc
limit 5