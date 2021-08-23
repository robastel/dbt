{{ config(materialized='ephemeral') }}

SELECT
    m.league_name
    , m.season_id
    , m.week
    , m.roster_id
    , m.points
    , m.opponent_roster_id
    , PERCENTILE_CONT(m.points, 0.5) OVER (PARTITION BY m.league_name, m.season_id, m.week) AS opponent_points
    , 1 AS is_median_matchup
    , m.is_regular_season_matchup
    , m.is_playoff_matchup
    , m.is_third_place_matchup
    , m.is_first_place_matchup
FROM
    {{ ref('matchups_h2h_raw') }} AS m
JOIN
    {{ ref('seasons') }} AS s 
    ON m.league_name = s.league_name
    AND m.season_id = s.season_id
    AND s.has_matchup_against_median = 1
WHERE
    m.is_regular_season_matchup = 1