{% materialization view, adapter='keboola' %}

  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='view') -%}
  {%- set existing_relation = load_cached_relation(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- If there's a table with this name, drop it first
  {% if existing_relation is not none and existing_relation.type == 'table' %}
    {{ log("Dropping table " ~ target_relation ~ " to make way for view", info=True) }}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
