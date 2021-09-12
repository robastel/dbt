WITH matchup_counts AS
(
    SELECT
        m.manager_id
        , m.manager_initials
        , m.league_name
        , COUNT(DISTINCT m.season_id) AS season_count
        , COALESCE(COUNT(DISTINCT CASE WHEN m.is_playoff_matchup = 1 THEN m.season_id END), 0) AS made_playoffs_count
        , COUNT(1) AS matchup_count
        , SUM(m.win) AS win_count
        , SUM(m.loss) AS loss_count
        , SUM(m.tie) AS tie_count
        , SUM(m.is_regular_season_matchup) AS regular_season_matchup_count
        , SUM(m.is_regular_season_matchup * m.win) AS regular_season_win_count
        , SUM(m.is_regular_season_matchup * m.loss) AS regular_season_loss_count
        , SUM(m.is_regular_season_matchup * m.tie) AS regular_season_tie_count
        , SUM(m.is_playoff_matchup) AS playoff_matchup_count
        , SUM(m.is_playoff_matchup * m.win) AS playoff_win_count
        , SUM(m.is_playoff_matchup * m.loss) AS playoff_loss_count
        , SUM(m.is_first_place_matchup * m.win) AS first_place_count
        , SUM(m.is_first_place_matchup * m.loss) AS second_place_count
        , SUM(m.is_third_place_matchup * m.win) AS third_place_count
    FROM
        {{ ref('fantasy_football__matchups') }} AS m
    WHERE
        m.is_completed = 1
    {{ dbt_utils.group_by(3) }}
)

, completed_regular_seasons AS
(
    SELECT
        m.season_id
        , min(is_completed) AS min_is_completed
    FROM
        {{ ref('fantasy_football__matchups') }} AS m
    WHERE
        m.is_regular_season_matchup = 1
    {{ dbt_utils.group_by(1) }}
    HAVING
        min_is_completed = 1
)

, regular_season_aggs AS
(
    SELECT
        m.manager_id
        , m.season_id
        , SUM(m.win) AS win_count
        , SUM(m.tie) AS tie_count
        , SUM(CASE WHEN m.is_median_matchup = 0 THEN m.points ELSE 0 END) AS season_points
        , MAX(CASE WHEN m.is_median_matchup = 0 THEN m.points ELSE 0 END) AS single_week_most_points
    FROM
        completed_regular_seasons AS crs
    JOIN
        {{ ref('fantasy_football__matchups') }} AS m
        ON crs.season_id = m.season_id
    WHERE
        m.is_regular_season_matchup = 1
    {{ dbt_utils.group_by(2) }}
)

, regular_season_records AS
(
    SELECT
        rsa.manager_id
        , rsa.season_id
        , ROW_NUMBER() OVER (
            PARTITION BY rsa.season_id
            ORDER BY rsa.win_count DESC, rsa.tie_count DESC, rsa.season_points DESC
          ) AS standing
        , ROW_NUMBER() OVER (
            PARTITION BY rsa.season_id
            ORDER BY rsa.season_points DESC
          ) AS points_standing
        , ROW_NUMBER() OVER (
            PARTITION BY rsa.season_id
            ORDER BY rsa.single_week_most_points DESC
          ) AS single_week_points_standing
    FROM
        regular_season_aggs AS rsa
)

, regular_season AS
(
    SELECT
        rsr.manager_id
        , COUNT(CASE WHEN rsr.standing = 1 THEN 1 END) AS regular_season_first_place_count
        , COUNT(CASE WHEN rsr.points_standing = 1 THEN 1 END) AS regular_season_most_points_count
        , COUNT(CASE WHEN rsr.single_week_points_standing = 1 THEN 1 END) AS regular_season_single_week_most_points_count
    FROM
        regular_season_records AS rsr
    {{ dbt_utils.group_by(1) }}
)

SELECT
    c.*
    , ROUND(((1.0 * c.win_count) + (0.5 * c.tie_count)) / c.matchup_count, 3) AS win_rate
    , ROUND(((1.0 * c.regular_season_win_count) + (0.5 * c.regular_season_tie_count)) / c.regular_season_matchup_count, 3) AS regular_season_win_rate
    , ROUND(1.0 * c.playoff_win_count / c.playoff_matchup_count, 3) AS playoff_win_rate
    , ROUND(1.0 * c.made_playoffs_count / c.season_count, 3) AS made_playoffs_rate
    , rs.regular_season_first_place_count
    , rs.regular_season_most_points_count
    , rs.regular_season_single_week_most_points_count
FROM
    matchup_counts AS c
LEFT JOIN
    regular_season AS rs
    ON c.manager_id = rs.manager_id
