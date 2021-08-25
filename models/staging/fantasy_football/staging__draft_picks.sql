SELECT * FROM {{ ref('base__espn_draft_picks') }}
UNION ALL
SELECT * FROM {{ ref('base__sleeper_draft_picks') }}