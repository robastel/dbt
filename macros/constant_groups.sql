{% test constant_groups(model, group_by_columns=[1], constant_columns=[1]) %}
    SELECT
        {{ group_by_columns|join(', ') }}
        , COUNT(DISTINCT {{ dbt_utils.surrogate_key(constant_columns) }}) AS distinct_count
    FROM
        {{ model }}
    GROUP BY
        {{ group_by_columns|join(', ') }}
    HAVING
        distinct_count > 1
{% endtest %}