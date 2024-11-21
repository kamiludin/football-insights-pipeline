with laliga_players AS (
    SELECT * FROM {{ source('football_2023', 'laliga_player_table') }}
)
SELECT
    *
FROM
    laliga_players