SELECT
    'sleeper' AS platform
    , season_id AS platform_season_id
    , week
    , matchup_id AS platform_matchup_id
    , roster_id AS platform_team_id
    , ROUND(points, 2) AS points
FROM
    {{ source('sleeper', 'matchups') }}