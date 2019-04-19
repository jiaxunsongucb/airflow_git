{% macro get_variables_from_seq(variables, seq, year, raw_data_schema) %}

    {%- call statement('obj_var_list', fetch_result=True) -%}
 
    	select VARID
    	from "CENSUS_DB"."{{raw_data_schema}}"."LOOKUP{{year}}5"
    	where VARID in ('{{variables|join("', '")}}') and SEQ = {{seq}}
 
    {%- endcall -%}
    
    {%- set column = load_result('obj_var_list')['data'] -%}
    {%- set var_list = [] -%}
    {%- for row in column -%}
        {{ var_list.append(row[0])  or ''}}
    {%- endfor -%}
    
	select STUSAB, LOGRECNO, {{year}} as year, {{var_list|join(', ')}}
	from "CENSUS_DB"."{{raw_data_schema}}"."E{{year}}5{{"%04d" | format(seq|int)}}"
	
{% endmacro %}