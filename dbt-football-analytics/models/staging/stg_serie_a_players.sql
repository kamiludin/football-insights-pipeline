with serie_a_players AS (
    SELECT * FROM {{ source('football_2023', 'serie_a_player_table') }}
)
SELECT
    *
FROM
    serie_a_players