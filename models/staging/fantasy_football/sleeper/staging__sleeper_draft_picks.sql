SELECT
    {{
        dbt_utils.generate_surrogate_key(
            ['platform', 'platform_season_id', 'platform_draft_id', 'overall_pick_num']
        )
    }} AS draft_pick_id
    , {{
        dbt_utils.generate_surrogate_key(
            ['platform', 'platform_season_id', 'platform_draft_id']
        ) 
      }} AS draft_id
    , platform
    , platform_season_id
    , platform_draft_id
    , platform_team_id
    , round_num
    , CASE
        WHEN MOD(round_num, 2) = 1 THEN draft_slot_num
        ELSE
            COUNT(platform_team_id) OVER (
                PARTITION BY platform, platform_season_id, platform_draft_id, round_num
            ) + 1 - draft_slot_num
      END AS round_pick_num
    , draft_slot_num
    , overall_pick_num
    , player_id
    , player_name
    , player_position
    , player_team
    , player_years_experience
    , is_keeper
FROM
    {{ ref('base__sleeper_draft_picks') }}