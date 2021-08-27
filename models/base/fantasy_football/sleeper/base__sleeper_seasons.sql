SELECT 
    'sleeper' AS platform
    , league_name
    , season_id AS platform_season_id
    , year
    , start_week
    , playoff_start_week - 1 AS regular_season_weeks 
    , last_completed_week
    , has_matchup_against_median
    , playoff_team_count
FROM
    {{ source('sleeper', 'seasons') }}