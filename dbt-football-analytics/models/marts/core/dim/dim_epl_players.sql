with dim_epl_players as (
    select * from {{ ref('stg_epl_players') }}
)
select
    id as player_id,
    player_name,
    "position" as positions,
    team_title
from
    dim_epl_players