SELECT
    {{ 
        dbt_utils.surrogate_key(
            ['platform', 'platform_season_id', 'platform_draft_id']
        ) 
    }} AS draft_id
    , *
FROM
    {{ ref('base__espn_draft_picks') }}