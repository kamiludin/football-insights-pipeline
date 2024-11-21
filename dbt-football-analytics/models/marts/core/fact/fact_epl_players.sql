with fact_epl_players as (
    select * from {{ ref('stg_epl_players') }}
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
    fact_epl_players