SELECT
    'sleeper' AS platform
    , season_id AS platform_season_id
    , bracket_round
    , winner_place
    , roster_id_a AS platform_team_id_a
    , roster_id_b AS platform_team_id_b
FROM
    {{ source('sleeper', 'lookup_playoffs') }}