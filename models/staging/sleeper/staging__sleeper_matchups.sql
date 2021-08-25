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
        {{ ref('base__sleeper_matchups') }}
)

, matchups_with_opp AS
(
    SELECT
        m.matchup_id
        , m.platform
        , ss.season_id
        , st.team_id
        , m.week
        , ss.league_name
        , m.points
        , m.platform_season_id
        , m.platform_team_id
        , opp.platform_team_id AS platform_opponent_team_id
        , ss.regular_season_weeks
        , ss.playoff_rounds
        , ss.total_weeks
    FROM
        matchups_with_key AS m
    JOIN 
        matchups_with_key AS opp
        ON m.platform_season_id = opp.platform_season_id
        AND m.week = opp.week
        AND m.platform_matchup_id = opp.platform_matchup_id
        AND m.platform_team_id <> opp.platform_team_id
        AND opp.platform_team_id IS NOT NULL
    LEFT JOIN 
        {{ ref('staging__sleeper_seasons')}} AS ss
        ON m.platform_season_id = ss.platform_season_id
    LEFT JOIN
        {{ ref('staging__sleeper_teams') }} as st
        ON m.platform_season_id = st.platform_season_id
        AND m.platform_team_id = st.platform_team_id
)

    ml.matchup_id
    , ml.platform
    , ml.season_id
    , ml.team_id
    , ml.week
    , ml.league_name
    , ml.points
    , ml.platform_season_id
    , ml.platform_team_id
    , ml.platform_opponent_team_id


SELECT
    mwo.matchup_id
    , mwo.platform
    , mwo.season_id
    , mwo.team_id
    , mwo.week
    , mwo.league_name
    , COALESCE(lmc.points, mwo.points) AS points
    , mwo.platform_season_id
    , mwo.platform_team_id
    , mwo.platform_opponent_team_id
    , CASE WHEN mwo.week <= mwo.regular_season_weeks THEN 1 ELSE 0 END AS is_regular_season_matchup
    , CASE WHEN mwo.week > mwo.regular_season_weeks THEN 1 ELSE 0 END AS is_playoff_matchup
    , CASE WHEN slp.winner_place = 3 THEN 1 ELSE 0 END AS is_third_place_matchup
    , CASE WHEN slp.winner_place = 1 THEN 1 ELSE 0 END AS is_first_place_matchup
FROM
    matchups_with_opp AS mwo
LEFT JOIN
    {{ ref('base__sleeper_lookup_playoffs') }} AS slp
    ON mwo.platform_season_id = slp.platform_season_id
    AND mwo.platform_team_id in (slp.platform_team_id_a, slp.platform_team_id_b)
    AND slp.bracket_round = mwo.week - mwo.regular_season_weeks
    -- The only losers bracket game we care about is the 3rd place game
    AND COALESCE(slp.winner_place, 0) <= 3
LEFT JOIN
    {{ ref('lookup_matchup_corrections') }} AS lmc
    ON mwo.league_name = lmc.league_name
    AND mwo.platform_season_id = lmc.platform_season_id
    AND mwo.week = lmc.week
    AND mwo.platform_team_id = lmc.platform_team_id
WHERE
    (mwo.week <= mwo.regular_season_weeks OR slp.platform_season_id IS NOT NULL)
