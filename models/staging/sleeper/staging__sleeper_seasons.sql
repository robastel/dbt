SELECT
    {{ 
        dbt_utils.surrogate_key(
            ['ss.platform', 'ss.platform_season_id']
        ) 
    }} AS season_id
    , ss.*
    , lps.rounds AS playoff_rounds
    , ss.regular_season_weeks + lps.rounds AS total_weeks
FROM
    {{ ref('base__sleeper_seasons') }} AS ss
LEFT JOIN
    {{ ref('lookup_playoff_structure')}} AS lps
    ON ss.playoff_team_count = lps.teams