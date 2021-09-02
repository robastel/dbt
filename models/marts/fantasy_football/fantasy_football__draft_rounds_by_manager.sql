SELECT
    manager_id
    , manager_initials
    , round_num
    , {{
        dbt_utils.pivot(
            column='player_position',
            values=['rb', 'wr', 'te', 'qb', 'def', 'k']
        )
      }}
FROM
    {{ ref('staging__draft_picks') }}
{{ dbt_utils.group_by(3) }}