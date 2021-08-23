WITH all_matchups AS
(
    SELECT 
        es.league_name
        , em.season_id
        , em.week
        , es.regular_season_weeks
        , es.playoff_rounds
        , es.regular_season_weeks + es.playoff_rounds AS total_weeks
        , em.team_id AS roster_id
        , ROUND(em.points, 2) AS points
        , em.opponent_id AS opponent_roster_id
        , SUM(CASE WHEN em.week > es.regular_season_weeks AND em.margin_of_victory < 0 AND et.standing <= es.playoff_team_count THEN 1 ELSE 0 END)
            OVER(
                PARTITION BY es.league_name, em.season_id, em.team_id 
                ORDER BY em.week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS playoff_losses
    FROM 
        {{ source('espn', 'matchups')}} AS em
    JOIN 
        {{ ref('espn_seasons')}} AS es
        ON em.season_id = es.season_id
    JOIN
        {{ source('espn', 'teams') }} AS et
        ON em.season_id = et.season_id
        AND em.team_id = et.team_id
    WHERE 
        (em.week <= es.regular_season_weeks OR et.standing <= es.playoff_team_count)
        AND em.team_id <> em.opponent_id
        AND em.opponent_id IS NOT NULL
)

, all_matchups_lag_playoff_losses AS
(
    SELECT
        am.*
        , LAG(am.playoff_losses) OVER(PARTITION BY am.league_name, am.season_id, am.roster_id ORDER BY am.week) AS lag_playoff_losses
        , LAG(am.playoff_losses, 2) OVER(PARTITION BY am.league_name, am.season_id, am.roster_id ORDER BY am.week) AS second_lag_playoff_losses
    FROM
        all_matchups am
)

SELECT
    amlpl.league_name
    , amlpl.season_id
    , amlpl.week
    , amlpl.roster_id
    , amlpl.points
    , amlpl.opponent_roster_id
    , CASE WHEN amlpl.week <= amlpl.regular_season_weeks THEN 1 ELSE 0 END AS is_regular_season_matchup
    , CASE WHEN amlpl.week > amlpl.regular_season_weeks THEN 1 ELSE 0 END AS is_playoff_matchup
    , CASE 
        WHEN amlpl.lag_playoff_losses = 1 
            AND amlpl.second_lag_playoff_losses = 0 
            AND amlpl.week = amlpl.total_weeks 
            THEN 1 ELSE 0
      END AS is_third_place_matchup
    , CASE 
        WHEN amlpl.lag_playoff_losses = 0 
            AND amlpl.week = amlpl.total_weeks 
            THEN 1 ELSE 0 
      END AS is_first_place_matchup
    -- , CASE
    --     WHEN amlpl.week <= amlpl.regular_season_weeks THEN 'regular_season'
    --     WHEN amlpl.lag_playoff_losses = 0 AND amlpl.week = amlpl.total_weeks THEN 'championship'
    --     WHEN amlpl.lag_playoff_losses = 1 AND amlpl.second_lag_playoff_losses = 0 AND amlpl.week = amlpl.total_weeks THEN 'third_place'
    --     WHEN amlpl.lag_playoff_losses = 0 AND amlpl.week = amlpl.total_weeks - 1 THEN 'semifinal'
    --     WHEN amlpl.lag_playoff_losses = 0 AND amlpl.week = amlpl.total_weeks - 2 THEN 'quarterfinal'
    --     ELSE 'earlier_playoff_rounds'
    --   END AS matchup_type
FROM
    all_matchups_lag_playoff_losses amlpl
WHERE
    COALESCE(amlpl.lag_playoff_losses, 0) = 0
    OR
    ( -- 3rd Place Game
        amlpl.week = amlpl.regular_season_weeks + amlpl.playoff_rounds
        AND amlpl.lag_playoff_losses = 1
        AND amlpl.second_lag_playoff_losses = 0
    )
