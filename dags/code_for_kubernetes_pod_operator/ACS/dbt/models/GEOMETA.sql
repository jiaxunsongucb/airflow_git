{%- set current_year = var("current_year", 2019) -%}
{%- set raw_data_schema = var("raw_data_schema", "TEST_RAW_ACS") -%}

{% for year in range(2009, current_year|int + 1) %}
    
    {% if adapter.get_relation("CENSUS_DB", raw_data_schema, "GEOMETA" + year|string + "5") %}
    	select {{ year }} as YEAR, GEOID, PLACE_NAME, STUSAB, State, County, Census_Tract, Block_Group, CBSA, ZCTA,
    		case when rlike(GEOID, '^(040|050|140|150).*') then substring(geoid, 8, 2) else 'NA' end as State_FIPS, 
    		case when rlike(GEOID, '^(050|140|150).*') then substring(geoid, 8, 5) else 'NA' end as County_FIPS, 
    		case when rlike(GEOID, '^(140|150).*') then substring(geoid, 8, 11) else 'NA' end as Census_Tract_FIPS, 
    		case when rlike(GEOID, '^(150).*') then substring(geoid, 8, 12) else 'NA' end as Block_Group_FIPS,
    		case when rlike(GEOID, '^(310).*') then substring(geoid, 8, 5) else 'NA' end as CBSA_Code,
    		case when rlike(GEOID, '^(040).*') then 'State'
    		     when rlike(GEOID, '^(050).*') then 'County'
    		     when rlike(GEOID, '^(140).*') then 'Census Tract'
    		     when rlike(GEOID, '^(150).*') then 'Block Group'
    		     when rlike(GEOID, '^(310).*') then 'CBSA'
    		     when rlike(GEOID, '^(860).*') then 'ZCTA'
    		     else 'NA'
    		end as GEO_LEVEL
    	from "CENSUS_DB"."{{ raw_data_schema }}"."GEOMETA{{year}}5"
    
        union all
    {% endif %}
{% endfor %}

select 9999, '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''
