{% macro handle_cdc_load(source, key_list, reversal) %}

    {% if reversal == 'X' %}

        select *, case when opflag = 'D' then 'X' else '' end as deletion_indicator
        from {{ source }}
        qualify
            row_number() over (

                partition by
                    {% for col in key_list %}
                        {{ col | lower() }} {%- if not loop.last %},{% endif %}
                    {% endfor -%}

                order by aedattm desc
            )
            = 1

    {% else %}

        select *, case when opflag = 'D' then 'X' else '' end as deletion_indicator
        from {{ source }}
        qualify
            row_number() over (

                partition by
                    {% for col in key_list %}
                        {{ col | lower() }} {%- if not loop.last %},{% endif %}
                    {% endfor -%}

                order by aedattm desc
            )
            = 1

    {% endif %}
{% endmacro %}
