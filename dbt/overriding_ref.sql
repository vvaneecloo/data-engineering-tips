{{ macro source() }}
    /*
        [To delete after copy pasting]
        Here you need to change this part of the code to make it work
        with your profile.yml file.
    */
    {% set node = builtins.source(model_name) %}

    {% if target.name  == 'dev' %} /* can also be profile.name depending on your use case */
        -- override source macro for {{ node }} as pipeline is launched in dev
        select * from {{ node }} limit 1000
    {% else %}
        {{ node }}
    {% endif %}
{% endmacro %}
