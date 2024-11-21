with dim_serie_a_players as (
    select * from {{ ref('stg_serie_a_players') }}
)
select
    id as player_id,
    player_name,
    "position" as positions,
    team_title
from
    dim_serie_a_players