{% set player_positions = ['rb', 'wr', 'te', 'qb', 'def', 'k'] %}

WITH manager_rounds_with_position_counts AS
(
    SELECT
        manager_id
        , manager_initials
        , round_num
        , {{
            dbt_utils.pivot(
                column='player_position',
                values=player_positions
            )
          }}
    FROM
        {{ ref('staging__draft_picks') }}
    {{ dbt_utils.group_by(3) }}
)

SELECT
    *
    , {{ player_positions|join(' + ') }} AS positions_sum
FROM
    manager_rounds_with_position_counts