{%- macro convert_to_date(column) -%}
    case
        when trim({{ column }}) = '0000-00-00' or trim({{ column }}) = '' or trim({{ column }}) is null
        then to_date('1900-01-01')
        else ifnull(try_to_date({{ column }}),to_date('1900-01-01'))
    end as {{ column }}
{%- endmacro -%}