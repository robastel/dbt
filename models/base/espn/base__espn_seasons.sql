SELECT 
    'espn' AS platform
    , league_name
    , season_id AS platform_season_id
    , year
    , start_week
    , regular_season_weeks 
    , current_week AS last_completed_week
    , 0 AS has_matchup_against_median
    , playoff_team_count
FROM
    {{ source('espn', 'seasons') }}