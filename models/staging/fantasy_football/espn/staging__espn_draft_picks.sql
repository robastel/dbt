WITH picks_without_primary_key AS
(
    SELECT
        platform
        , platform_season_id
        , platform_draft_id
        , platform_team_id
        , round_num
        , round_pick_num
        , CASE
            WHEN MOD(round_num, 2) = 1 THEN round_pick_num
            ELSE
                MAX(round_pick_num) OVER (
                    PARTITION BY platform, platform_season_id, platform_draft_id
                ) + 1 - round_pick_num
          END AS draft_slot_num
        , (round_num - 1)
            * MAX(round_pick_num) OVER (
                PARTITION BY platform, platform_season_id, platform_draft_id
              )
            + round_pick_num AS overall_pick_num
        , player_id
        , player_name
        , player_position
        , player_team
        , player_years_experience
        , is_keeper
    FROM
        {{ ref('base__espn_draft_picks') }}
)

SELECT
    {{
        dbt_utils.surrogate_key(
            ['platform', 'platform_season_id', 'platform_draft_id', 'overall_pick_num']
        )
    }} AS draft_pick_id
    , {{
        dbt_utils.surrogate_key(
            ['platform', 'platform_season_id', 'platform_draft_id']
        )
      }} AS draft_id
    , *
FROM
    picks_without_primary_key