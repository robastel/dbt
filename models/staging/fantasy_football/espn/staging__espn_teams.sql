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
        {{ ref('base__espn_teams') }}
)

SELECT 
    t.team_id
    , t.platform
    , t.platform_season_id
    , t.platform_team_id
    , es.season_id
    , es.league_name
    , lm.manager_id
    , lm.platform_manager_id
    , lm.manager_initials
    , t.regular_season_standing
FROM
    teams_with_key AS t
JOIN
    {{ ref('staging__espn_seasons') }} AS es
    ON t.platform_season_id = es.platform_season_id
JOIN
    {{ ref('lookup__managers') }} AS lm
    ON t.platform_team_id = lm.platform_manager_id
    AND es.league_name = lm.league_name
    AND lm.platform = 'espn'