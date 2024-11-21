with dim_ligue1_players as (
    select * from {{ ref('stg_ligue1_players') }}
)
select
    id as player_id,
    player_name,
    "position" as positions,
    team_title
from
    dim_ligue1_players