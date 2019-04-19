select *
from
({{ get_variables_merge_geoid_all_years( var("variables", ["B00001_001"]), var("select_col", "B00001_001 as total_population"), var("roofstock_names", "total_population"), var("start_year", "2009"), var("end_year", "2017"), var("raw_data_schema", "TEST_RAW_ACS") ) }})