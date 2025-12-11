{% macro keboola__drop_relation_if_exists(relation) %}
  {% if relation is not none %}
    {{ adapter.drop_relation(relation) }}
  {% endif %}
{% endmacro %}


{% macro keboola__get_create_table_as_sql(temporary, relation, sql) -%}
  {# Always use CREATE OR REPLACE for Keboola/Snowflake #}
  create or replace {% if temporary -%}temporary {% endif -%}table {{ relation }} as (
    {{ sql }}
  )
{%- endmacro %}


{% macro keboola__create_table_as(temporary, relation, compiled_sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace {% if temporary -%}
    temporary
  {%- endif %} table {{ relation }}
  as (
    {{ compiled_sql }}
  );
{%- endmacro %}


{% macro keboola__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro keboola__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{%- endmacro %}


{% macro keboola__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{%- endmacro %}


{% macro keboola__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{%- endmacro %}


{% macro keboola__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}


{% macro keboola__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
      "column_name",
      "data_type",
      "character_maximum_length",
      "numeric_precision",
      "numeric_scale"
    from {{ relation.information_schema('columns') }}
    where "table_name" = '{{ relation.identifier }}'
      {% if relation.schema %}
      and "table_schema" = '{{ relation.schema }}'
      {% endif %}
    order by "ordinal_position"
  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}


{% macro keboola__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro keboola__get_or_create_relation(database, schema, identifier, type) -%}
  {%- set target_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}

  {% do return(target_relation) %}
{% endmacro %}
