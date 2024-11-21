with dim_laliga_league as (
    select
    id as match_id,

    {{ parse_json_columns(ref('stg_laliga_league'), {
        'h': ['id', 'title', 'short_title'],
        'a': ['id', 'title', 'short_title']
    }) }},

    datetime
    from
        {{ ref('stg_laliga_league') }}
)

select * from dim_laliga_league
