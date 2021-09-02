SELECT
    'sleeper' AS platform
    , season_id AS platform_season_id
    , draft_id AS platform_draft_id
    , roster_id AS platform_team_id
    , round_num
    , NULL AS round_pick_num
    , draft_slot AS draft_slot_num
    , pick_num AS overall_pick_num
    , CONCAT('sleeper_', player_id) AS player_id
    , LOWER(CONCAT(first_name, ' ', last_name)) AS player_name
    , LOWER(position) AS player_position
    , LOWER(team) AS player_team
    , years_exp AS player_years_experience
    , is_keeper
FROM
    {{ source('sleeper', 'draft_picks') }}