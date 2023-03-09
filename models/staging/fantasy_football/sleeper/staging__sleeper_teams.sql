WITH teams_with_key AS
(
    SELECT
        {{ 
            dbt_utils.generate_surrogate_key(
                ['platform', 'platform_season_id', 'platform_team_id']
            ) 
        }} AS team_id
        , *
    FROM
        {{ ref('base__sleeper_rosters') }}
)

SELECT
    t.team_id
    , t.platform
    , t.platform_season_id
    , t.platform_team_id
    , ss.season_id
    , ss.league_name
    , lm.manager_id
    , lm.platform_manager_id
    , lm.manager_initials
FROM
    teams_with_key AS t
JOIN
    {{ ref('staging__sleeper_seasons') }} ss
    ON t.platform_season_id = ss.platform_season_id
JOIN
    {{ ref('base__sleeper_rosters') }} sr
    ON ss.platform_season_id = sr.platform_season_id
    AND t.platform_team_id = sr.platform_team_id
JOIN
    {{ ref('lookup__managers') }} lm
    ON sr.user_id = CAST(lm.platform_manager_id AS STRING)
    AND ss.league_name = lm.league_name
    AND lm.platform = 'sleeper'