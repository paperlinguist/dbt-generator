{# This macro collects data from a query and inserts it into a list. See https://stackoverflow.com/questions/74898764/iterate-over-all-rows-and-columns-in-dbt-jinja #}

{% macro macro_snapshot_list(source_system) %}
    {% set query_to_process %}
        select *
        from {{ ref( 'dbt_snapshot_seed' ) }}
        where system_name = '{{source_system}}'
    {% endset %}

    {% set results = dbt_utils.get_query_results_as_dict(query_to_process) %}

    {% if execute %}
    {% set results_list = results.rows %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ print(results) }}

{% endmacro %}