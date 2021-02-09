WITH all_matchups AS
(
    SELECT * FROM {{ ref('espn_matchups') }}
    UNION ALL
    SELECT * FROM {{ ref('sleeper_matchups') }}
)

SELECT
    am.league_name
    , am.season_id
    , am.week
    , am.roster_id
    , am.points
    , am.opponent_roster_id
    , opp.points opponent_points
    , ROUND(am.points - opp.points, 2) AS point_differential
    , CASE WHEN ROUND(am.points - opp.points, 2) > 0 THEN 1 ELSE 0 END win
    , CASE WHEN ROUND(am.points - opp.points, 2) < 0 THEN 1 ELSE 0 END loss
    , CASE WHEN ROUND(am.points - opp.points, 2) = 0 THEN 1 ELSE 0 END tie
    , am.is_regular_season_matchup
    , am.is_playoff_matchup
    , am.matchup_type
FROM
    all_matchups am
JOIN
    all_matchups opp
    ON am.opponent_roster_id = opp.roster_id
    AND am.week = opp.week
    AND am.season_id = opp.season_id
    AND am.league_name = opp.league_name
