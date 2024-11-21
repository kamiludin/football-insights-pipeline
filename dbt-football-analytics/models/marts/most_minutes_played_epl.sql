with dim_epl_players as (
    select * from {{ ref('dim_epl_players') }}
),

fact_epl_players as (
    select * from {{ ref('fact_epl_players') }}
)

select
    de.player_id,
    de.player_name,
    de.team_title,
    de.positions,
    fe.minutes_played
from
    dim_epl_players de
join
    fact_epl_players fe
on de.player_id = fe.player_id
where positions != 'GK'
order by fe.minutes_played desc
limit 5