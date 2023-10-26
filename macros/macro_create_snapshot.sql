{% macro macro_create_snapshot(snapshot_name, dbt_source, table_name, unique_key, snapshot_strategy, updated_at, check_cols, invalidate_hard_deletes, composite_key) %}

    {%- if composite_key == 1 %}
        {% set comp_key = unique_key|replace(";"," ||'-'|| ") %}
        {% set unique_key = 'dbt_composite_key' %}
    {% endif -%}

    {%- set reserved_words = dbt_utils.get_column_values(table=ref('sql_reserved_words'), column='reserved_word') -%}
    {%- set unique_key_upper = unique_key|upper %}
    {%- if unique_key_upper in reserved_words -%}
        {% set reserved_word_flag = 1 %} {% set new_unique_key = unique_key ~ "_" %}
    {%- endif -%}

    {%- set snp_sql %}
{% raw %}{% snapshot{% endraw %} {{snapshot_name}} {% raw %}%}{% endraw %}
{% raw %}{{{% endraw %}
    {% raw %}config({% endraw %}
        {%- if reserved_word_flag == 1 -%}
            {% set unique_key = new_unique_key %}
        {%- endif -%}
        unique_key = "{{unique_key}}",
        strategy = "{{snapshot_strategy}}",
        {% if updated_at != "None" -%}
            updated_at = "{{updated_at}}",
        {% endif -%}
        {% if check_cols != "None" -%}
            check_cols = "{{check_cols}}",
        {% endif -%}
        {% if invalidate_hard_deletes == 1 -%}
            invalidate_hard_deletes = True
        {% endif -%}

    {% raw %}){% endraw %}
{% raw %}}}{% endraw %}

{% if composite_key == 1 %}
    select {{comp_key}} as dbt_composite_key, * from {% raw %}{{ source({% endraw %}'{{ dbt_source }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
{% elif reserved_word_flag == 1 %}
    select "{{unique_key_upper}}" as {{new_unique_key}}, * exclude ("{{unique_key_upper}}") from {% raw %}{{ source({% endraw %}'{{ dbt_source }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
{% else %}
    select * from {% raw %}{{ source({% endraw %}'{{ dbt_source }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
{% endif %}

{% raw %}{% endsnapshot %}{% endraw %}
    {% endset %}

    {% if execute %} {{ log(snp_sql, info=True) }} {% do return(snp_sql) %} {% endif %}
{% endmacro %}
