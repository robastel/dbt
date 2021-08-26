SELECT
    'sleeper' AS platform
    , season_id AS platform_season_id
    , user_id
FROM
    {{ source('sleeper', 'user_seasons')}}