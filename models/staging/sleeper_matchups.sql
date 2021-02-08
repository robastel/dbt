WITH all_matchups AS
(
    SELECT
        ss.league_name
        , ss.season_id
        , sm.week
        , ss.regular_season_weeks
        , sm.roster_id
        , ROUND(sm.points, 2) AS points
        , o.roster_id AS opponent_roster_id
    FROM
        {{ source('sleeper', 'matchups') }} AS sm
    JOIN 
        {{ ref('sleeper_seasons')}} AS ss
        ON sm.season_id = ss.season_id
    JOIN 
        {{ source('sleeper', 'matchups') }} AS o
        ON sm.season_id = o.season_id
        AND sm.week = o.week
        AND sm.matchup_id = o.matchup_id
        AND sm.roster_id <> o.roster_id
)

SELECT
    am.league_name
    , am.season_id
    , am.week
    , am.roster_id
    , am.points
    , am.opponent_roster_id
FROM
    all_matchups am
WHERE
    am.week <= am.regular_season_weeks
    OR EXISTS
    (
        SELECT
            1
        FROM
            {{ source('sleeper', 'lookup_playoffs') }} sls
        WHERE
            sls.season_id = am.season_id
            AND am.roster_id in (sls.roster_id_a, sls.roster_id_b)
            AND sls.bracket_round = am.week - am.regular_season_weeks
            -- The only losers bracket game we care about is the 3rd place game
            AND COALESCE(sls.winner_place, 0) <= 3
    )
