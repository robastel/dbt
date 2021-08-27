SELECT 
    {{ 
        dbt_utils.star(
            ref('staging__espn_teams'), 
            except=["regular_season_standing"]
        ) 
    }} 
FROM
    {{ ref('staging__espn_teams') }}

UNION ALL

SELECT 
    *
FROM 
    {{ ref('staging__sleeper_teams') }}
