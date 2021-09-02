WITH unioned_draft_picks AS
(
    SELECT * FROM {{ ref('staging__espn_draft_picks') }}
    UNION ALL
    SELECT * FROM {{ ref('staging__sleeper_draft_picks') }}
)

SELECT
    dp.draft_pick_id
    , dp.draft_id
    , s.league_name
    , s.season_id
    , t.manager_id
    , t.manager_initials
    , s.year
    , dp.platform
    , dp.platform_season_id
    , dp.platform_draft_id
    , dp.platform_team_id
    , dp.round_num
    , dp.round_pick_num
    , dp.draft_slot_num
    , dp.overall_pick_num
    , dp.player_id
    , dp.player_name
    , dp.player_position
    , dp.player_team
    , dp.player_years_experience
    , dp.is_keeper
FROM
    unioned_draft_picks AS dp
LEFT JOIN
    {{ ref('staging__teams') }} AS t
    ON dp.platform_season_id = t.platform_season_id
    AND dp.platform_team_id = t.platform_team_id
LEFT JOIN
    {{ ref('staging__seasons') }} AS s
    ON dp.platform_season_id = s.platform_season_id