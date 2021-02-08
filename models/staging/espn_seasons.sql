SELECT 
    es.league_name
    , es.season_id
    , es.year
    , es.start_week
    , es.regular_season_weeks 
    , es.current_week AS last_completed_week
    , 0 AS has_matchup_against_median
    , es.playoff_team_count
    , lps.rounds AS playoff_rounds
FROM
    {{ source('espn', 'seasons') }} es
LEFT JOIN
    {{ ref('lookup_playoff_structure')}} lps
    ON es.playoff_team_count = lps.teams