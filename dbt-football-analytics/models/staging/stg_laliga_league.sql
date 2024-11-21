with laliga_league AS (
    SELECT * FROM {{ source('football_2023', 'laliga_league_table') }}
)
SELECT
    *
FROM
    laliga_league