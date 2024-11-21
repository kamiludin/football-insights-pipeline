with serie_a_league AS (
    SELECT * FROM {{ source('football_2023', 'serie_a_league_table') }}
)
SELECT
    *
FROM
    serie_a_league