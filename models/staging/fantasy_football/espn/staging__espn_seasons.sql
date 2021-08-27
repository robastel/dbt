SELECT
    {{ 
        dbt_utils.surrogate_key(
            ['es.platform', 'es.platform_season_id']
        ) 
    }} AS season_id
    , es.*
    , lps.rounds AS playoff_rounds
    , es.regular_season_weeks + lps.rounds AS total_weeks
FROM
    {{ ref('base__espn_seasons') }} AS es
LEFT JOIN
    {{ ref('lookup__playoff_structure')}} AS lps
    ON es.playoff_team_count = lps.teams