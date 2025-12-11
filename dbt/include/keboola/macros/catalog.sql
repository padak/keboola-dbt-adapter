{% macro keboola__get_catalog(information_schema, schemas) -%}
  {% set query %}
    with tables as (
      select
        TABLE_CATALOG as table_database,
        TABLE_SCHEMA,
        TABLE_NAME,
        TABLE_TYPE
      from {{ information_schema }}.tables
      where (
        {%- for schema in schemas -%}
          upper(TABLE_SCHEMA) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
    ),

    columns as (
      select
        TABLE_CATALOG as table_database,
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        ORDINAL_POSITION as column_index,
        DATA_TYPE as column_type
      from {{ information_schema }}.columns
      where (
        {%- for schema in schemas -%}
          upper(TABLE_SCHEMA) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
    )

    select
      columns.table_database,
      columns.TABLE_SCHEMA,
      columns.TABLE_NAME,
      tables.TABLE_TYPE,
      columns.COLUMN_NAME,
      columns.column_index,
      columns.column_type
    from tables
    join columns
      on tables.table_database = columns.table_database
      and tables.TABLE_SCHEMA = columns.TABLE_SCHEMA
      and tables.TABLE_NAME = columns.TABLE_NAME
    order by
      columns.column_index
  {% endset %}

  {{ return(run_query(query)) }}
{%- endmacro %}


{% macro keboola__list_schemas(database) -%}
  {% set sql %}
    select SCHEMA_NAME
    from {{ information_schema_name(database) }}.schemata
  {% endset %}
  {{ return(run_query(sql)) }}
{%- endmacro %}


{% macro keboola__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
    select count(*)
    from {{ information_schema }}.schemata
    where upper(SCHEMA_NAME) = upper('{{ schema }}')
  {%- endset %}
  {{ return(run_query(sql)) }}
{%- endmacro %}


{% macro keboola__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      TABLE_CATALOG as database,
      TABLE_SCHEMA as schema,
      TABLE_NAME as name,
      case
        when TABLE_TYPE = 'BASE TABLE' then 'table'
        when TABLE_TYPE = 'VIEW' then 'view'
        else lower(TABLE_TYPE)
      end as type
    from {{ schema_relation.information_schema('tables') }}
    where TABLE_SCHEMA = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro keboola__information_schema_name(database) -%}
  {% if database -%}
    {{ adapter.quote_as_configured(database, 'database') }}.information_schema
  {%- else -%}
    information_schema
  {%- endif -%}
{%- endmacro %}


{% macro keboola__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {% endcall %}
{%- endmacro %}


{% macro keboola__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {% endcall %}
{%- endmacro %}
