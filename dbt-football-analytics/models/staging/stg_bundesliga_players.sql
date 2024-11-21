with bundesliga_players AS (
    SELECT * FROM {{ source('football_2023', 'bundesliga_player_table') }}
)
SELECT
    *
FROM
    bundesliga_players