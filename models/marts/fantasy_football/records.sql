WITH matchup_counts AS
(
    SELECT
        m.league_name
        , m.manager_id
        , m.manager_initials
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
        {{ ref('staging__matchups') }} AS m
    {{ dbt_utils.group_by(3) }}
)

SELECT
    c.*
    , ROUND(((1.0 * c.win_count) + (0.5 * c.tie_count)) / c.matchup_count, 3) AS win_rate
    , ROUND(((1.0 * c.regular_season_win_count) + (0.5 * c.regular_season_tie_count)) / c.regular_season_matchup_count, 3) AS regular_season_win_rate
    , ROUND(1.0 * c.playoff_win_count / c.playoff_matchup_count, 3) AS playoff_win_rate
    , ROUND(1.0 * c.made_playoffs_count / c.season_count, 3) AS made_playoffs_rate
FROM
    matchup_counts AS c
