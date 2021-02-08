WITH all_matchups AS
(
    SELECT 
        es.league_name
        , em.season_id
        , em.week
        , es.regular_season_weeks
        , es.playoff_rounds
        , em.team_id AS roster_id
        , ROUND(em.points, 2) AS points
        , em.opponent_id AS opponent_roster_id
        , SUM(CASE WHEN em.week > regular_season_weeks AND em.margin_of_victory < 0 AND et.standing <= es.playoff_team_count THEN 1 ELSE 0 END)
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
FROM
    all_matchups_lag_playoff_losses amlpl
WHERE
    amlpl.playoff_losses = 0
    OR
    ( -- 3rd Place Game
        amlpl.week = amlpl.regular_season_weeks + amlpl.playoff_rounds
        AND amlpl.lag_playoff_losses = 1
        AND amlpl.second_lag_playoff_losses = 0
    )
