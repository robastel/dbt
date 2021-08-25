SELECT
    'sleeper' AS platform
    , sm.season_id AS platform_season_id
    , sm.week
    , sm.matchup_id AS platform_matchup_id
    , sm.roster_id AS platform_team_id
    , ROUND(sm.points, 2) AS points
FROM
    {{ source('sleeper', 'matchups') }}