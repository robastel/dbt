SELECT
    'sleeper' AS platform
    , season_id AS platform_season_id
    , roster_id AS platform_team_id
    , user_id
FROM
    {{ source('sleeper', 'rosters') }}