with fact_laliga_players as (
    select * from {{ ref('stg_laliga_players') }}
)
select
    id as player_id,
    player_name,
    games,
    "time" as minutes_played,
    goals,
    xg,
    assists,
    xa,
    shots,
    key_passes,
    yellow_cards,
    red_cards,
    npg,
    npxg,
    xgchain,
    xgbuildup
from
    fact_laliga_players