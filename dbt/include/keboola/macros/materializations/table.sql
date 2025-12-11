{% materialization table, adapter='keboola' %}

  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Use CREATE OR REPLACE to handle existing tables
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as (
      {{ sql }}
    )
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  -- Update cache
  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
