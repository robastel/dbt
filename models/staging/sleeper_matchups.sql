WITH all_matchups AS
(
    SELECT
        ss.league_name
        , ss.season_id
        , sm.week
        , ss.regular_season_weeks
        , ss.playoff_rounds
        , ss.regular_season_weeks + ss.playoff_rounds AS total_weeks
        , sm.roster_id
        , ROUND(sm.points, 2) AS points
        , opp.roster_id AS opponent_roster_id
    FROM
        {{ source('sleeper', 'matchups') }} AS sm
    JOIN 
        {{ ref('sleeper_seasons')}} AS ss
        ON sm.season_id = ss.season_id
    JOIN 
        {{ source('sleeper', 'matchups') }} AS opp
        ON sm.season_id = opp.season_id
        AND sm.week = opp.week
        AND sm.matchup_id = opp.matchup_id
        AND sm.roster_id <> opp.roster_id
        AND opp.roster_id IS NOT NULL
)

SELECT
    am.league_name
    , am.season_id
    , am.week
    , am.roster_id
    , COALESCE(lmc.points, am.points) AS points
    , am.opponent_roster_id
    , CASE WHEN am.week <= am.regular_season_weeks THEN 1 ELSE 0 END AS is_regular_season_matchup
    , CASE WHEN am.week > am.regular_season_weeks THEN 1 ELSE 0 END AS is_playoff_matchup
    , CASE WHEN slp.winner_place = 3 THEN 1 ELSE 0 END AS is_third_place_matchup
    , CASE WHEN slp.winner_place = 1 THEN 1 ELSE 0 END AS is_first_place_matchup
    -- , CASE
    --     WHEN am.week <= am.regular_season_weeks THEN 'regular_season'
    --     WHEN slp.winner_place = 1 THEN 'championship'
    --     WHEN slp.winner_place = 3 THEN 'third_place'
    --     WHEN am.week = am.total_weeks - 1 THEN 'semifinal'
    --     WHEN am.week = am.total_weeks - 2 THEN 'quarterfinal'
    --     ELSE 'earlier_playoff_rounds'
    --   END AS matchup_type
FROM
    all_matchups AS am
LEFT JOIN
    {{ source('sleeper', 'lookup_playoffs') }} AS slp
    ON am.season_id = slp.season_id
    AND am.roster_id in (slp.roster_id_a, slp.roster_id_b)
    AND slp.bracket_round = am.week - am.regular_season_weeks
    -- The only losers bracket game we care about is the 3rd place game
    AND COALESCE(slp.winner_place, 0) <= 3
LEFT JOIN
    {{ ref('lookup_matchup_corrections') }} AS lmc
    ON am.league_name = lmc.league_name
    AND am.season_id = lmc.season_id
    AND am.week = lmc.week
    AND am.roster_id = lmc.roster_id
WHERE
    (am.week <= am.regular_season_weeks OR slp.season_id IS NOT NULL)
