WITH matchups_h2h AS
(
    SELECT * FROM {{ ref('staging__espn_matchups') }}
    UNION ALL
    SELECT * FROM {{ ref('staging__sleeper_matchups') }}
)

, matchups_with_opp AS
(
    SELECT
        m.matchup_id
        , m.platform
        , m.season_id
        , m.team_id
        , m.manager_id
        , m.manager_initials
        , m.week
        , m.league_name
        , m.points
        , opp.team_id AS opponent_team_id
        , opp.points AS opponent_points
        , ROUND(m.points - opp.points, 2) AS point_differential
        , 0 AS is_median_matchup
        , m.is_regular_season_matchup
        , m.is_playoff_matchup
        , m.is_third_place_matchup
        , m.is_first_place_matchup
        , m.platform_season_id
        , m.platform_team_id
        , m.plaform_manager_id
        , m.platform_opponent_team_id
    FROM
        matchups_h2h AS m
    JOIN
        matchups_h2h AS opp
        ON m.season_id = opp.season_id
        AND m.week = opp.week
        AND m.platform_opponent_team_id = opp.platform_team_id
)

SELECT
    matchup_id
    , platform
    , season_id
    , team_id
    , manager_id
    , manager_initials
    , week
    , league_name
    , points
    , opponent_team_id
    , opponent_points
    , point_differential
    , CASE WHEN point_differential > 0 THEN 1 ELSE 0 END AS win
    , CASE WHEN point_differential < 0 THEN 1 ELSE 0 END AS loss
    , CASE WHEN point_differential = 0 THEN 1 ELSE 0 END AS tie
    , is_median_matchup
    , is_regular_season_matchup
    , is_playoff_matchup
    , is_third_place_matchup
    , is_first_place_matchup
    , platform_season_id
    , platform_team_id
    , platform_manager_id
    , platform_opponent_team_id
FROM
    matchups_with_opp
