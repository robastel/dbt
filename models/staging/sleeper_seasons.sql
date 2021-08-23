SELECT 
    ss.league_name
    , ss.season_id
    , ss.year
    , ss.start_week
    , ss.playoff_start_week - 1 AS regular_season_weeks 
    , ss.last_completed_week
    , ss.has_matchup_against_median
    , ss.playoff_team_count
    , lps.rounds AS playoff_rounds
FROM
    {{ source('sleeper', 'seasons') }} ss
LEFT JOIN
    {{ ref('lookup_playoff_structure')}} lps
    ON ss.playoff_team_count = lps.teams