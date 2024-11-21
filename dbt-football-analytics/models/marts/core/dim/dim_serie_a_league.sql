with dim_serie_a_league as (
    select
    id as match_id,

    {{ parse_json_columns(ref('stg_serie_a_league'), {
        'h': ['id', 'title', 'short_title'],
        'a': ['id', 'title', 'short_title']
    }) }},

    datetime
    from
        {{ ref('stg_serie_a_league') }}
)

select * from dim_serie_a_league