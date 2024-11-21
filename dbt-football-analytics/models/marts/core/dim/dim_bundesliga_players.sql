with dim_bundesliga_players as (
    select * from {{ ref('stg_bundesliga_players') }}
)
select
    id as player_id,
    player_name,
    "position" as positions,
    team_title
from
    dim_bundesliga_players