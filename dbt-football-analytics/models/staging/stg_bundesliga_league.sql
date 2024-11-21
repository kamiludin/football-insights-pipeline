with bundesliga_league AS (
    SELECT * FROM {{ source('football_2023', 'bundesliga_league_table') }}
)
SELECT
    *
FROM
    bundesliga_league