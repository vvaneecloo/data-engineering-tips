{{ macro source() }}
    /*
        [To delete after copy pasting]
        Here you need to change this part of the code to make it work
        with your profile.yml file.
    */
    {% set node = builtins.ref(model_name) %}

    {% if profile.name  == 'dev' %}
        -- override source macro for {{ node }} as pipeline is launched in dev
        select * from {{ node }} limit 1000
    {% else %}
        {{ node }}
    {% endif %}
{% endmacro %}