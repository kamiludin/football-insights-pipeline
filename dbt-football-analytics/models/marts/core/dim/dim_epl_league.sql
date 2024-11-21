with dim_epl_league as (
    select
    id as match_id,

    {{ parse_json_columns(ref('stg_epl_league'), {
        'h': ['id', 'title', 'short_title'],
        'a': ['id', 'title', 'short_title']
    }) }},

    datetime
    from
        {{ ref('stg_epl_league') }}
)

select * from dim_epl_league
