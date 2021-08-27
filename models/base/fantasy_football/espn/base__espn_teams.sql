SELECT
    'espn' AS platform
    , season_id AS platform_season_id
    , team_id AS platform_team_id
    , standing AS regular_season_standing
FROM
    {{ source('espn', 'teams') }}