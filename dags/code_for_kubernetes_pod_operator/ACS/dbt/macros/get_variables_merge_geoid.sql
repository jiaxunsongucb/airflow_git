{% macro get_variables_merge_geoid(variables, select_col, year, roofstock_names) %}

    select data.year, geo.GEOID, {{roofstock_names}}
    from ({{ get_variables( variables, select_col, year ) }}) as data
    right join "CENSUS_DB"."RAW_ACS"."GEOMETA{{year}}5" as geo
    on lower(data.STUSAB) = lower(geo.STUSAB) and data.LOGRECNO = geo.LOGRECNO 

{% endmacro %}