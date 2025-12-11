{% materialization incremental, adapter='keboola' %}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set incremental_strategy = config.get('incremental_strategy', 'merge') -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_cached_relation(target_relation) -%}
  {%- set tmp_relation = make_temp_relation(target_relation, '__dbt_tmp') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {# Table doesn't exist - do a full refresh #}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

  {% elif existing_relation.is_view or should_full_refresh() %}
    {# Relation is a view or full refresh is requested - drop and recreate #}
    {{ adapter.drop_relation(existing_relation) }}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

  {% else %}
    {# Incremental run - build temp table and merge #}
    {% call statement('create_tmp_relation') -%}
      {{ create_table_as(True, tmp_relation, sql) }}
    {%- endcall %}

    {% if incremental_strategy == 'merge' %}
      {{ keboola__incremental_merge(target_relation, tmp_relation, unique_key) }}
    {% elif incremental_strategy == 'delete+insert' %}
      {{ keboola__incremental_delete_insert(target_relation, tmp_relation, unique_key) }}
    {% elif incremental_strategy == 'append' %}
      {{ keboola__incremental_append(target_relation, tmp_relation) }}
    {% else %}
      {{ exceptions.raise_compiler_error('Invalid incremental_strategy: ' ~ incremental_strategy) }}
    {% endif %}

    {{ adapter.drop_relation(tmp_relation) }}

  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro keboola__incremental_merge(target_relation, tmp_relation, unique_key) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {% if unique_key is not none %}
    {# Use MERGE statement #}
    {% call statement('merge') -%}
      merge into {{ target_relation }} as target
      using {{ tmp_relation }} as source
      on (
        {% if unique_key is string %}
          target."{{ unique_key }}" = source."{{ unique_key }}"
        {% elif unique_key is iterable %}
          {% for key in unique_key %}
            target."{{ key }}" = source."{{ key }}"
            {%- if not loop.last %} and {% endif -%}
          {% endfor %}
        {% endif %}
      )
      when matched then update set
        {% for column in dest_columns %}
          "{{ column.name }}" = source."{{ column.name }}"
          {%- if not loop.last %}, {% endif -%}
        {% endfor %}
      when not matched then insert
        ({{ dest_cols_csv }})
      values
        ({% for column in dest_columns %}source."{{ column.name }}"{%- if not loop.last %}, {% endif -%}{% endfor %})
    {%- endcall %}
  {% else %}
    {# No unique key - just append #}
    {{ keboola__incremental_append(target_relation, tmp_relation) }}
  {% endif %}
{% endmacro %}


{% macro keboola__incremental_delete_insert(target_relation, tmp_relation, unique_key) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {% if unique_key is not none %}
    {# Delete matching rows then insert #}
    {% call statement('delete') -%}
      delete from {{ target_relation }}
      where (
        {% if unique_key is string %}
          "{{ unique_key }}"
        {% elif unique_key is iterable %}
          ({% for key in unique_key %}"{{ key }}"{%- if not loop.last %}, {% endif -%}{% endfor %})
        {% endif %}
      ) in (
        select
          {% if unique_key is string %}
            "{{ unique_key }}"
          {% elif unique_key is iterable %}
            {% for key in unique_key %}"{{ key }}"{%- if not loop.last %}, {% endif -%}{% endfor %}
          {% endif %}
        from {{ tmp_relation }}
      )
    {%- endcall %}
  {% endif %}

  {# Insert all rows from temp table #}
  {% call statement('insert') -%}
    insert into {{ target_relation }} ({{ dest_cols_csv }})
    select {{ dest_cols_csv }}
    from {{ tmp_relation }}
  {%- endcall %}
{% endmacro %}


{% macro keboola__incremental_append(target_relation, tmp_relation) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {% call statement('insert') -%}
    insert into {{ target_relation }} ({{ dest_cols_csv }})
    select {{ dest_cols_csv }}
    from {{ tmp_relation }}
  {%- endcall %}
{% endmacro %}
