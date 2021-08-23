SELECT * FROM {{ ref('espn_draft_picks') }}
UNION ALL
SELECT * FROM {{ ref('sleeper_draft_picks') }}