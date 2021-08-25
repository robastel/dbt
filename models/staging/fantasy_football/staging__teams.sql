SELECT * FROM {{ ref('staging__espn_teams') }}
UNION ALL
SELECT * FROM {{ ref('staging__sleeper_teams') }}
