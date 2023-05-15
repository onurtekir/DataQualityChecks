from tdq_engine.tdq_engine import TDQEngine
from rule_definitions import rule_definitions
from tdq_engine.tdq_configuration import TDQConfiguration
from tdq_engine.tdq_google_cloud_configuration import TDQGoogleCloudConfiguration

gcp_config = TDQGoogleCloudConfiguration(project_id="trv-data-tenant-df-stage",
                                         dataset_id="otekir",
                                         tdq_summary_table="cdp_checks_summary",
                                         tdq_results_table="cdp_checks_results")

tdq_config = TDQConfiguration(tdq_check_name="Test DQ checks",
                              tdq_check_description="Data quality checks for testing",
                              tdq_check_parameters={"ymd": 20230510})


engine = TDQEngine(gcp_configuration=gcp_config, dq_check_configuration=tdq_config)

query = f"""

    WITH cte_impression AS (
        
        SELECT 
            ymd,
            poll_request_id,
            ARRAY_LENGTH(visible_item_list) AS visible_item_list_count,
            page_number,
            room_count,
            concept_search_type_v3,
            application_id
        FROM
            `trv-dci-data-stage.imp_phase_1.impression` AS imp
        LIMIT 
          100
            
    )
    
    SELECT
        *
    FROM
        cte_impression

"""

rules = []
rules.append(rule_definitions.check_NOT_NULL(column_name="poll_request_id"))
rules.append(rule_definitions.check_IN(column_name="room_count", values=[1]))
rules.append(rule_definitions.check_STRING_CONTAINS(column_name="application_id", search_value="WEB_APP", case_sensitive=True))
rules.append(rule_definitions.check_GREATER_THAN(column_name=None, value=1, or_equal=False))

engine.run_data_quality_checks(base_query=query, tdq_rules=rules)
