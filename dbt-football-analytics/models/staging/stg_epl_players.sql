with epl_players AS (
    SELECT * FROM {{ source('football_2023', 'epl_player_table') }}
)
SELECT
    *
FROM
    epl_players