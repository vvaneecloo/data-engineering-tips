/*
    This macro aims to update the comment dict in dbt to pass more informations such as 
    Airflow dag id / run id.

    To use this in your dbt project you'd have to:
        - pass the variables `AIRFLOW_DAG_ID` & `AIRFLOW_RUN_ID` as environment variables
        - update the names of your environments ('preprod' / 'prod')
        - the rest is handled automatically by dbt
*/

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
