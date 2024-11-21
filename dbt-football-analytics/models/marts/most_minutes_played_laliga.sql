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
    dl.positions,
    fl.minutes_played
from
    dim_laliga_players dl
join
    fact_laliga_players fl
on dl.player_id = fl.player_id
where positions != 'GK'
order by fl.minutes_played desc
limit 5