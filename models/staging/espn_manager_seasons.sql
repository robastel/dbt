SELECT
    es.league_name
    , es.season_id
    , lm.manager_id
    , lm.platform_manager_id
    , lm.platform_manager_id AS roster_id
    , lm.manager_initials
FROM
    {{ ref('espn_seasons') }} es
JOIN
    {{ source('espn', 'teams')}} et
    ON es.season_id = et.season_id
JOIN
    {{ ref('lookup_managers') }} lm
    ON et.team_id = lm.platform_manager_id
    AND es.league_name = lm.league_name
    AND lm.platform = 'espn'