WITH all_matchups AS
(
    SELECT * FROM {{ ref('matchups_h2h') }}
    UNION ALL 
    SELECT * FROM {{ ref('matchups_median') }}
)

SELECT
    *
    , ROUND(points - opponent_points, 2) AS point_differential
    , CASE WHEN ROUND(points - opponent_points, 2) > 0 THEN 1 ELSE 0 END AS win
    , CASE WHEN ROUND(points - opponent_points, 2) < 0 THEN 1 ELSE 0 END AS loss
    , CASE WHEN ROUND(points - opponent_points, 2) = 0 THEN 1 ELSE 0 END AS tie
FROM
    all_matchups