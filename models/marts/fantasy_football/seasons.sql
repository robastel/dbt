SELECT * FROM {{ ref('espn_seasons') }}
UNION ALL
SELECT * FROM {{ ref('sleeper_seasons') }}