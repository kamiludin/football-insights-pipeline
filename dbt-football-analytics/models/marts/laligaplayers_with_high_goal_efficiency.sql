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
    fl.shots,
    (CAST(fl.goals AS FLOAT) / fl.shots) as goal_efficiency
from
    dim_laliga_players dl
join
    fact_laliga_players fl
on dl.player_id = fl.player_id
where fl.goals > 15
order by goal_efficiency desc
limit 5