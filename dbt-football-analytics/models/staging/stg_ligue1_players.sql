with ligue1_players AS (
    SELECT * FROM {{ source('football_2023', 'ligue1_player_table') }}
)
SELECT
    *
FROM
    ligue1_players