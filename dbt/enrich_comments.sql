{% macro enrich_comment(node) %}
    {%- set comment_dict = {} -%}
    {%- do comment_dict.update(
        env=target.name,
        app='dbt',
        version=dbt_version,
        profile_name=target.get('profile_name'),
    ) -%}
    {%- if node is not none and target.name in ["preprod", "prod"]-%}
      {%- do comment_dict.update(
        dag=env_var('AIRFLOW_DAG_ID', ''),
        dag_execution_time=env_var('AIRFLOW_RUN_ID', ''),
        node_id=node.unique_id,
        node_name=node.name,
        resource_type=node.resource_type,
        package_name=node.package_name,
        relation={
            "database": node.database,
            "schema": node.schema,
            "identifier": node.identifier
        },
      ) -%}
    {% else %}
      {%- do comment_dict.update(node_id='internal') -%}
    {%- endif -%}
    {% do return(tojson(comment_dict)) %}
{% endmacro %}
