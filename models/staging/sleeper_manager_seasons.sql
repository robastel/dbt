SELECT
    ss.league_name
    , ss.season_id
    , lm.manager_id
    , lm.platform_manager_id
    , sr.roster_id
    , lm.manager_initials
FROM
    {{ ref('sleeper_seasons') }} ss
JOIN
    {{ source('sleeper', 'user_seasons')}} sus
    ON ss.season_id = sus.season_id
JOIN
    {{ source('sleeper', 'rosters') }} sr
    ON sus.user_id = sr.user_id
    AND sus.season_id = sr.season_id
JOIN
    {{ ref('lookup_managers') }} lm
    ON sus.user_id = CAST(lm.platform_manager_id AS STRING)
    AND ss.league_name = lm.league_name
    AND lm.platform = 'sleeper'