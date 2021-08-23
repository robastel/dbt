SELECT * FROM {{ ref('espn_manager_seasons') }}
UNION ALL
SELECT * FROM {{ ref('sleeper_manager_seasons') }}