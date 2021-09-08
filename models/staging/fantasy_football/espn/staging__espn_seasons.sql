SELECT
    {{ 
        dbt_utils.surrogate_key(
            ['es.platform', 'es.platform_season_id']
        ) 
    }} AS season_id
    , es.platform
    , es.league_name
    , es.platform_season_id
    , es.year
    , es.start_week
    , es.regular_season_weeks
    , es.regular_season_weeks + lps.rounds AS last_completed_week
    , es.has_matchup_against_median
    , es.playoff_team_count
    , lps.rounds AS playoff_rounds
    , es.regular_season_weeks + lps.rounds AS total_weeks
FROM
    {{ ref('base__espn_seasons') }} AS es
LEFT JOIN
    {{ ref('lookup__playoff_structure')}} AS lps
    ON es.playoff_team_count = lps.teams