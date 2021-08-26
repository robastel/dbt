SELECT * FROM {{ ref('staging__espn_draft_picks') }}
UNION ALL
SELECT * FROM {{ ref('staging__sleeper_draft_picks') }}