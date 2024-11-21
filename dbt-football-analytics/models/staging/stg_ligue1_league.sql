with ligue1_league AS (
    SELECT * FROM {{ source('football_2023', 'ligue1_league_table') }}
)
SELECT
    *
FROM
    ligue1_league