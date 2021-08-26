WITH matchups_with_median AS
(
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
        , NULL AS opponent_team_id
        , ROUND(
            PERCENTILE_CONT(m.points, 0.5) OVER (
                PARTITION BY league_name, season_id, week
            ) 
            , 3
          ) AS opponent_points
        , 1 AS is_median_matchup
        , is_regular_season_matchup
        , is_playoff_matchup
        , is_third_place_matchup
        , is_first_place_matchup
        , platform_season_id
        , platform_team_id
        , platform_manager_id
        , platform_opponent_team_id
    FROM
        {{ ref('staging__matchups_h2h') }} h2h
    JOIN
        {{ ref('staging__seasons') }} AS s 
        ON h2h.season_id = s.season_id
        AND s.has_matchup_against_median = 1
    WHERE
        is_regular_season_matchup = 1
)

, median_matchups_with_point_differential
(
    SELECT
        *
        , ROUND(points - opponent_points, 3) AS point_differential
    FROM
        matchups_with_median
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
    median_matchups_with_point_differential
