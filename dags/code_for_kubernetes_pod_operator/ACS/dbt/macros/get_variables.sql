{% macro get_variables(variables, select_col, year, raw_data_schema) %}

    {%- call statement('obj_seq_list', fetch_result=True) -%}

    	select distinct SEQ
    	from "CENSUS_DB"."{{raw_data_schema}}"."LOOKUP{{year}}5"
    	where VARID in ('{{variables|join("', '")}}')

    {%- endcall -%}
    
    {%- set column = load_result('obj_seq_list')['data'] -%}
    {%- set seq_list = [] -%}
    {%- for row in column -%}
        {{ seq_list.append(row[0])  or ''}}
    {%- endfor -%}

    {%- set first_seq = seq_list[:1]|first|string -%}
    {%- set prev_seq = first_seq -%}
        select e{{first_seq}}.STUSAB, e{{first_seq}}.LOGRECNO, e{{first_seq}}.year, {{select_col}}
        from ({{ get_variables_from_seq( variables, first_seq, year, raw_data_schema ) }}) as e{{first_seq}}
    {% if seq_list|length > 1 %}
        {% for seq in seq_list[1:] %}
            full outer join ({{ get_variables_from_seq( variables, seq, year, raw_data_schema ) }}) as e{{seq}}
            on e{{prev_seq}}.STUSAB = e{{seq}}.STUSAB and e{{prev_seq}}.LOGRECNO = e{{seq}}.LOGRECNO
            {%- set prev_seq = seq -%}
        {% endfor %}
    {% endif %}
 
{% endmacro %}