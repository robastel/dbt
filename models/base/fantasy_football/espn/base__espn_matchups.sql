SELECT 
    'espn' AS platform
    , season_id AS platform_season_id
    , team_id AS platform_team_id
    , week
    , ROUND(points, 2) AS points
    , opponent_id AS platform_opponent_team_id
    , ROUND(margin_of_victory, 2) AS point_differential
FROM 
    {{ source('espn', 'matchups')}}