SELECT
    'espn' AS platform
    , season_id AS platform_season_id
    , team_id AS platform_team_id
FROM
    {{ source('espn', 'teams') }}