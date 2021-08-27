WITH matchups_with_key AS
(
    SELECT
        {{ 
            dbt_utils.surrogate_key(
                ['platform', 'platform_season_id', 'platform_team_id', 'week']
            ) 
        }} AS matchup_id
        , *
    FROM
        {{ ref('base__espn_matchups') }}
)

, matchups_with_season_and_team AS
(
    SELECT 
        m.matchup_id
        , m.platform
        , es.season_id
        , et.team_id
        , et.manager_id
        , et.manager_initials
        , m.week
        , es.league_name
        , m.points
        , m.platform_season_id
        , m.platform_team_id
        , et.platform_manager_id
        , m.platform_opponent_team_id
        , es.regular_season_weeks
        , es.playoff_rounds
        , es.total_weeks
        , SUM(
            CASE 
                WHEN m.week > es.regular_season_weeks 
                    AND m.point_differential < 0 
                    AND et.regular_season_standing <= es.playoff_team_count 
                    THEN 1 ELSE 0 
            END
            )
            OVER (
                PARTITION BY es.season_id, et.team_id 
                ORDER BY m.week 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS playoff_losses
    FROM
        matchups_with_key AS m
    JOIN 
        {{ ref('staging__espn_seasons')}} AS es
        ON m.platform_season_id = es.platform_season_id
    JOIN
        {{ ref('staging__espn_teams') }} AS et
        ON m.platform_season_id = et.platform_season_id
        AND m.platform_team_id = et.platform_team_id
    WHERE 
        (
            m.week <= es.regular_season_weeks 
            OR 
            et.regular_season_standing <= es.playoff_team_count
        )
        AND m.platform_team_id <> m.platform_opponent_team_id
        AND m.platform_opponent_team_id IS NOT NULL
)

, matchups_lag_playoff_losses AS
(
    SELECT
        m.*
        , LAG(m.playoff_losses) OVER (
            PARTITION BY m.season_id, m.platform_team_id 
            ORDER BY m.week
          ) AS lag_playoff_losses
        , LAG(m.playoff_losses, 2) OVER (
            PARTITION BY m.season_id, m.platform_team_id 
            ORDER BY m.week
          ) AS second_lag_playoff_losses
    FROM
        matchups_with_season_and_team AS m
)

SELECT
    ml.matchup_id
    , ml.platform
    , ml.season_id
    , ml.team_id
    , ml.manager_id
    , ml.manager_initials
    , ml.week
    , ml.league_name
    , ml.points
    , ml.platform_season_id
    , ml.platform_team_id
    , ml.platform_manager_id
    , ml.platform_opponent_team_id
    , CASE 
        WHEN ml.week <= ml.regular_season_weeks 
            THEN 1 ELSE 0 
      END AS is_regular_season_matchup
    , CASE 
        WHEN ml.week > ml.regular_season_weeks 
            THEN 1 ELSE 0 
      END AS is_playoff_matchup
    , CASE 
        WHEN ml.lag_playoff_losses = 1 
            AND ml.second_lag_playoff_losses = 0 
            AND ml.week = ml.total_weeks 
            THEN 1 ELSE 0
      END AS is_third_place_matchup
    , CASE 
        WHEN ml.lag_playoff_losses = 0 
            AND ml.week = ml.total_weeks 
            THEN 1 ELSE 0 
      END AS is_first_place_matchup
FROM
    matchups_lag_playoff_losses ml
WHERE
    COALESCE(ml.lag_playoff_losses, 0) = 0
    OR
    ( -- 3rd Place Game
        ml.week = ml.total_weeks
        AND ml.lag_playoff_losses = 1
        AND ml.second_lag_playoff_losses = 0
    )