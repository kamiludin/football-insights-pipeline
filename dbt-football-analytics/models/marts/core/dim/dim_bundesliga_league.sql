with dim_bundesliga_league as (
    select
    id as match_id,

    {{ parse_json_columns(ref('stg_bundesliga_league'), {
        'h': ['id', 'title', 'short_title'],
        'a': ['id', 'title', 'short_title']
    }) }},

    datetime
    from
        {{ ref('stg_bundesliga_league') }}
)

select * from dim_bundesliga_league
