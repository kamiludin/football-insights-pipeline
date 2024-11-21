with fact_serie_a_league as (
    select
    id as match_id,
    {{ parse_json_columns(ref('stg_serie_a_league'), {
        'h': ['id', 'title'],
        'a': ['id', 'title'],
        'goals': ['h', 'a'],
        'xG': ['h', 'a'],
        'forecast': ['w', 'd', 'l']
    }) }}
    from
        {{ ref('stg_serie_a_league') }}
)

select * from fact_serie_a_league