SELECT * FROM {{ ref('staging__espn_seasons') }}
UNION ALL
SELECT * FROM {{ ref('staging__sleeper_seasons') }}