{% macro macro_create_soft_delete_snapshot(snapshot_name, invalidate_fivetran_soft_deletes, invalidate_soft_deletes, soft_delete_indicator_col, soft_delete_date_col) %}
    {%- if (invalidate_fivetran_soft_deletes == 1) and (invalidate_soft_deletes == 1) -%}
        {%- set valid_to -%}
        case
            when source.{{soft_delete_indicator_col}} = True then source.{{soft_delete_date_col}}
            when source.dbt_valid_to <> null then source.dbt_valid_to
            when source._fivetran_deleted = True then source._fivetran_synced
            else null
        end as dbt_valid_to
        {%- endset -%}
    {%- elif invalidate_fivetran_soft_deletes == 1 -%}
        {%- set valid_to -%}
        case
            when source.dbt_valid_to <> null
            then source.dbt_valid_to
            when source._fivetran_deleted = true
            then source._fivetran_synced
            else null
        end as dbt_valid_to
        {%- endset -%}
    {%- elif invalidate_soft_deletes == 1 -%}
        {%- set valid_to -%}
        case
            when source.{{ soft_delete_indicator_col }} = true
            then source.{{ soft_delete_date_col }}
            when source.dbt_valid_to <> null
            then source.dbt_valid_to
            else null
        end as dbt_valid_to
        {%- endset -%}
    {%- endif -%}

    {%- set snp_sql -%}
    select
        source.* exclude (dbt_valid_to),
        {{valid_to}}
    from {% raw %}{{ ref({% endraw %}'{{ snapshot_name }}'{% raw %}) }}{% endraw %} source
    {%- endset -%}

    {%- if execute %}
        {{ log(snp_sql, info=True) }} {% do return(snp_sql) %}
    {% endif -%}
{% endmacro %}
