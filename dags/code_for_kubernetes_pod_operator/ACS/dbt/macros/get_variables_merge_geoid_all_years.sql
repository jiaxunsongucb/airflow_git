{% macro get_variables_merge_geoid_all_years(variables, select_col, roofstock_names, start_year, end_year, raw_data_schema) %}
    
    {{get_variables_merge_geoid(variables, select_col, start_year|string, roofstock_names, raw_data_schema)}}
    {% for year in range(start_year|int + 1, end_year|int + 1) %}
        union all
        {{get_variables_merge_geoid(variables, select_col, year|string, roofstock_names, raw_data_schema)}}
    {% endfor %}
    
{% endmacro %}