SELECT * FROM {{ ref('staging__matchups_h2h') }}
UNION ALL
SELECT * FROM {{ ref('staging__matchups_median') }}