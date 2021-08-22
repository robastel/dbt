{{ config(materialized='ephemeral') }}

SELECT
    m.league_name
    , m.season_id
    , m.week
    , m.roster_id
    , m.points
    , m.opponent_roster_id
    , opp.points AS opponent_points
    , 0 AS is_median_matchup
    , m.is_regular_season_matchup
    , m.is_playoff_matchup
    , m.is_third_place_matchup
    , m.is_first_place_matchup
FROM
    {{ ref('matchups_h2h_raw') }} AS m
JOIN
    {{ ref('matchups_h2h_raw') }} AS opp
    ON m.opponent_roster_id = opp.roster_id
    AND m.week = opp.week
    AND m.season_id = opp.season_id
    AND m.league_name = opp.league_name