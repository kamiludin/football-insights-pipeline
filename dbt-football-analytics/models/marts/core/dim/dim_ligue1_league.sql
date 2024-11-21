with dim_ligue1_league as (
    select
    id as match_id,

    {{ parse_json_columns(ref('stg_ligue1_league'), {
        'h': ['id', 'title', 'short_title'],
        'a': ['id', 'title', 'short_title']
    }) }},

    datetime
    from
        {{ ref('stg_ligue1_league') }}
)

select * from dim_ligue1_league
