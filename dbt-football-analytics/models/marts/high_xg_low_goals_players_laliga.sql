with dim_laliga_players as (
    select * from {{ ref('dim_laliga_players') }}
),

fact_laliga_players as (
    select * from {{ ref('fact_laliga_players') }}
)

select
    dl.player_id,
    dl.player_name,
    dl.team_title,
    fl.goals,
    fl.xg,
    (CAST(fl.xg AS FLOAT) - fl.goals) as goal_diff
from
    dim_laliga_players dl
join
    fact_laliga_players fl
on dl.player_id = fl.player_id
where fl.xg > fl.goals
order by goal_diff desc
limit 5