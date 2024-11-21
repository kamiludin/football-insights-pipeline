with epl_league AS (
    SELECT * FROM {{ source('football_2023', 'epl_league_table') }}
)
SELECT
    *
FROM
    epl_league