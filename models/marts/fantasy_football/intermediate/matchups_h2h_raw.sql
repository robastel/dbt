SELECT * FROM {{ ref('espn_matchups') }}
UNION ALL
SELECT * FROM {{ ref('sleeper_matchups') }}