{% macro add_missing_master_data(table_column, target_table, target_key) %}

    {%- set source_relation = adapter.get_relation(database=this.database, schema='conformed', identifier=target_table|lower()) -%}
    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- set column_name = [] -%}

    {% for i in columns %}
        {% do column_name.append(i.column) %}
    {% endfor %}

    {%- set filtered_columns = [] -%}
    {% for i in column_name %}
        {% if 'key' in i|lower() and 'datasource' not in i | lower() and target_key|lower() not in i|lower() %}
            {% do filtered_columns.append(i|lower()) %}
        {% else %}
            {% continue %}
        {% endif %}
    {% endfor %}

    {%- set missing_master_data_source_flag = namespace(missing_master_data_source_flag = '') -%}

    {% for i in column_name %}
        {% if 'missing_master_data_source' in i|lower()%}
            {% set missing_master_data_source_flag.missing_master_data_source_flag = ''%}
            {% break %}
        {% else %}
            {% set missing_master_data_source_flag.missing_master_data_source_flag = 'X'%}
            {% continue %}
        {% endif %}
    {% endfor %}

    {%- set conformed_database_name = namespace(conformed_database_name = target.database) -%}

    {% if missing_master_data_source_flag.missing_master_data_source_flag == 'X'%}
        {% set add_column = run_query('alter table '~conformed_database_name.conformed_database_name~'.conformed.'~target_table~' add column missing_master_data_source varchar(50)') %}
    {% else %}
    {% endif %}

    {% if execute %}
        {%- set max_timestamp = run_query('select max(edw_load_timestamp) from '~this.database~'.conformed.'~target_table).columns[0].values()[0] -%}
    {% else %}
    {% endif %}

    {% if target.name == 'develop' %}
        {% set raw_database_name = namespace(raw_database_name = 'SAP_DEV') %}
    {% elif target.name == 'staging' %} 
        {% set raw_database_name = namespace(raw_database_name = 'SAP_QA') %}
    {% else %}
        {% set raw_database_name = namespace(raw_database_name = 'SAP_PROD') %}
    {% endif %}

    {% set insert_statment = namespace(insert_statment ='')%}
    {% set union_parts = namespace(union_parts='')%}

    {% for dict in table_column %}

        {% set final_target_select = namespace(final_target_select='') %}
        {% set final_key_select = namespace(final_key_select='')%}
        {% set final_key_assign = namespace(final_key_assign='')%}
        {% set template_v = namespace(template_v = '')%}

        {% for table, columns in dict.items() %}
            {% set join_statements = [] %}
            {% set as_statements = [] %}
            {% set source_select = [] %}
            {% set target_select = [] %}
            {% set table = table %}
            {% set columns = columns %}

            {% for source_columns, target_columns in columns.items() %}
                {% do join_statements.append(source_columns+' = '+target_columns) %}
                {% do as_statements.append(source_columns+' as '+target_columns) %}
                {% do source_select.append(source_columns) %}
                {% do target_select.append(target_columns) %}
            {% endfor %}
            
            {% set final_source_select = namespace(final_source_select='') %}
            {% for i in source_select %}
                {% if loop.index == source_select|length %}
                    {% set final_source_select.final_source_select = final_source_select.final_source_select~i %}
                {% else %} 
                    {% set final_source_select.final_source_select = final_source_select.final_source_select~i~',' %}
                {% endif %}
            {% endfor %}
            
            {% set source_select_as = namespace(source_select_as='') %}
            {% for i in as_statements %}
                {% if loop.index == as_statements|length %}
                    {% set source_select_as.source_select_as = source_select_as.source_select_as~i %}
                {% else %} 
                    {% set source_select_as.source_select_as = source_select_as.source_select_as~i~',' %}
                {% endif %}
            {% endfor %}

            {% for i in target_select %}
                {% if loop.index == target_select|length %}
                    {% set final_target_select.final_target_select = final_target_select.final_target_select~i %}
                {% else %} 
                    {% set final_target_select.final_target_select = final_target_select.final_target_select~i~',' %}
                {% endif %}
            {% endfor %}

            {% set final_join_statement = namespace(final_join_statement=' ') %}
            {% for i in join_statements %}
                {% if loop.index == join_statements|length %}
                    {% set final_join_statement.final_join_statement = final_join_statement.final_join_statement~i %}
                {% else %} 
                    {% set final_join_statement.final_join_statement = final_join_statement.final_join_statement~i~' and ' %}
                {% endif %}
            {% endfor %}

            {% for i in filtered_columns %}
                {% set final_key_select.final_key_select = final_key_select.final_key_select~i~','%}
                {% set final_key_assign.final_key_assign = final_key_assign.final_key_assign~'-1 as '~i~','%}
            {% endfor %}

            {% set key = namespace(key='') %}
            {% for i in source_select %}
                {% if loop.index == source_select|length %}
                    {% set key.key = key.key~i %}
                {% else %} 
                    {% set key.key = key.key~i~'||' %}
                {% endif %}
            {% endfor %}

            {% set template_v.template_v = 'select distinct md5_number_upper64('~key.key~') as '~target_key~','~source_select_as.source_select_as~", current_timestamp as dbt_run_timestamp, '"~max_timestamp~"' as edw_load_timestamp, 10153719269126660984 as datasource_key, '"~table~"' as missing_master_data_source from ( select distinct "~ final_source_select.final_source_select~' from raw.' ~ raw_database_name.raw_database_name ~ '.'~table~' left join ' ~ conformed_database_name.conformed_database_name ~ '.conformed.'~target_table~' on '~ final_join_statement.final_join_statement ~' where dbt_run_timestamp is null and ('~key.key~') is not null)' %}

        {% endfor %}

    {% if loop.index == table_column|length %}
        {% set union_parts.union_parts = union_parts.union_parts ~ template_v.template_v%}
    {% else %}
        {% set union_parts.union_parts = union_parts.union_parts + template_v.template_v + ' union ' %}
    {% endif %}

    {% set insert_statment.insert_statment = 'merge into '~ conformed_database_name.conformed_database_name ~ '.conformed.'~target_table~' as target using ( select '~target_key~','~final_target_select.final_target_select~','~final_key_assign.final_key_assign~'dbt_run_timestamp, edw_load_timestamp, datasource_key, missing_master_data_source from ('~ union_parts.union_parts ~')) as source on target.'~target_key~'= source.'~target_key~' when not matched then insert ('~target_key~','~final_target_select.final_target_select~','~final_key_select.final_key_select~'dbt_run_timestamp, edw_load_timestamp, datasource_key, missing_master_data_source) values ('~target_key~','~final_target_select.final_target_select~','~final_key_select.final_key_select~'dbt_run_timestamp, edw_load_timestamp, datasource_key, missing_master_data_source)'%}

    {% endfor %}

    {% set results = run_query(insert_statment.insert_statment) %}

{% endmacro %}
