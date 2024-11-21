with dim_laliga_players as (
    select * from {{ ref('stg_laliga_players') }}
)
select
    id as player_id,
    player_name,
    "position" as positions,
    team_title
from
    dim_laliga_players