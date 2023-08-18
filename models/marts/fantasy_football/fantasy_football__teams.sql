WITH team_agg AS (
    SELECT
        t.team_id
        , t.platform
        , t.platform_season_id
        , t.platform_team_id
        , t.season_id
        , s.year
        , t.league_name
        , t.manager_id
        , t.platform_manager_id
        , t.manager_initials
        , SUM(m.win) AS regular_season_win_count
        , SUM(m.loss) AS regular_season_loss_count
        , SUM(m.tie) AS regular_season_tie_count
        , SUM(CASE WHEN m.is_regular_season_matchup = 1 AND m.is_median_matchup = 0 THEN m.points ELSE 0 END) AS regular_season_points
        , MAX(CASE WHEN m.is_regular_season_matchup = 1 AND m.is_median_matchup = 0 THEN m.points ELSE 0 END) AS regular_season_single_week_high_points
        , SUM(m.is_first_place_matchup * m.win) AS is_first_place
        , SUM(m.is_first_place_matchup * m.loss) AS is_second_place
        , SUM(m.is_third_place_matchup * m.win) AS is_third_place
        , SUM(m.is_third_place_matchup * m.loss) AS is_fourth_place
    FROM
        {{ ref('staging__teams') }} AS t
    LEFT JOIN
        {{ ref('staging__seasons') }} AS s
        ON t.season_id = s.season_id
    LEFT JOIN
        {{ ref('fantasy_football__matchups') }} AS m
        ON t.team_id = m.team_id
    {{ dbt_utils.group_by(10) }}
)

, teams_with_regular_season_standing AS (
    SELECT
        team_id
        , platform
        , platform_season_id
        , platform_team_id
        , season_id
        , year
        , league_name
        , manager_id
        , platform_manager_id
        , manager_initials
        , regular_season_win_count
        , regular_season_loss_count
        , regular_season_tie_count
        , regular_season_points
        , regular_season_single_week_high_points
        , RANK() OVER(
            PARTITION BY season_id
            ORDER BY regular_season_win_count DESC, regular_season_tie_count DESC, regular_season_points DESC
        ) AS regular_season_standing
        , is_first_place
        , is_second_place
        , is_third_place
        , is_fourth_place
    FROM
        team_agg AS ta
)

, teams_with_final_standing AS (
    SELECT
        team_id
        , platform
        , platform_season_id
        , platform_team_id
        , season_id
        , year
        , league_name
        , manager_id
        , platform_manager_id
        , manager_initials
        , regular_season_win_count
        , regular_season_loss_count
        , regular_season_tie_count
        , regular_season_points
        , regular_season_single_week_high_points
        , CASE
            WHEN ROW_NUMBER() OVER(
                PARTITION BY season_id
                ORDER BY regular_season_single_week_high_points DESC
            ) = 1 THEN 1
            ELSE 0
        END AS is_regular_season_single_week_most_points
        , regular_season_standing
        , ROW_NUMBER() OVER(
            PARTITION BY season_id
            ORDER BY
                CASE
                    WHEN is_first_place = 1 THEN -4
                    WHEN is_second_place = 1 THEN -3
                    WHEN is_third_place = 1 THEN -2
                    WHEN is_fourth_place = 1 THEN -1
                    ELSE regular_season_standing
                END
        ) AS final_standing

    FROM
        teams_with_regular_season_standing
)

SELECT
    t.*
    , (
        s.default_cost
        + CASE
            WHEN t.final_standing = 1 THEN s.first_award
            WHEN t.final_standing = 2 THEN s.second_award
            WHEN t.final_standing = 3 THEN s.third_award
        END
        + CASE
            WHEN t.regular_season_standing = 1 THEN s.regular_season_first_award
            WHEN t.regular_season_standing = 10 THEN s.tenth_cost
            WHEN t.regular_season_standing = 11 THEN s.eleventh_cost
            WHEN t.regular_season_standing = 12 THEN s.twelfth_cost
        END
        + t.is_regular_season_single_week_most_points * s.regular_season_single_week_most_points_award
    ) AS season_rating
FROM
    teams_with_final_standing AS t
LEFT JOIN
    {{ ref('lookup__seasons') }} AS s
    ON t.season_id = s.season_id