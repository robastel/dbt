SELECT
    'espn' AS platform
    , season_id AS platform_season_id
    , season_id AS platform_draft_id
    , team_id AS platform_team_id
    , round_num
    , round_pick AS round_pick_num
    , NULL AS draft_slot_num
    , NULL AS overall_pick_num
    , CONCAT('espn_', CAST(player_id AS STRING)) AS player_id
    , player_name
    , CAST(NULL AS STRING) AS player_position
    , CAST(NULL AS STRING) AS player_team
    , CAST(NULL AS STRING) AS player_years_experience
    , is_keeper
FROM
    {{ source('espn', 'draft_picks') }}