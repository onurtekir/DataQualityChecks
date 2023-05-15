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
            parent_poll_request_id,
            ARRAY_LENGTH(visible_item_list) AS visible_item_list_count,
            page_number,
            room_count,
            concept_search_type_v3,
            application_id,
            IF(ARRAY_LENGTH(visible_item_list) > 0, True, False) AS vil_valid
        FROM
            `trv-dci-data-stage.imp_phase_1.impression` AS imp
        -- WHERE ymd = '2023-12-01'
            
    )
    
    SELECT
        *
    FROM
        cte_impression

"""

rules = []

# region NULL
rule_null_parent_poll_request_id = rule_definitions.check_NULL(column_name="parent_poll_request_id")
print(rule_null_parent_poll_request_id.getRuleSQL())
rules.append(rule_null_parent_poll_request_id)
# engine.test_rule(base_query=query, tdq_rule=rule_null_parent_poll_request_id, row_limit=10)
# endregion

# region NOT NULL
rule_not_null_poll_request_id = rule_definitions.check_NOT_NULL(column_name="poll_request_id")
print(rule_not_null_poll_request_id.getRuleSQL())
rules.append(rule_not_null_poll_request_id)
# engine.test_rule(base_query=query, tdq_rule=rule_not_null_poll_request_id, row_limit=10)
# endregion

# region NULL_OR_EMPTY
rule_null_or_empty = rule_definitions.check_NULL_OR_EMPTY(column_name="parent_poll_request_id", is_trimmed=True)
print(rule_null_or_empty.getRuleSQL())
rules.append(rule_null_or_empty)
# engine.test_rule(base_query=query, tdq_rule=rule_null_or_empty, row_limit=10)
# endregion

# region GREATER_THAN
rule_greater_than = rule_definitions.check_GREATER_THAN(column_name="room_count", or_equal=True, value=2)
print(rule_greater_than.getRuleSQL())
rules.append(rule_greater_than)
# engine.test_rule(base_query=query, tdq_rule=rule_greater_than, row_limit=1000)
# endregion

# region LESS_THAN
rule_less_than = rule_definitions.check_LESS_THAN(column_name="room_count", or_equal=True, value=2)
print(rule_less_than.getRuleSQL())
rules.append(rule_less_than)
# engine.test_rule(base_query=query, tdq_rule=rule_less_than, row_limit=1000)
# endregion

# region BETWEEN
rule_between = rule_definitions.check_BETWEEN(column_name="room_count", min_value=2, max_value=4)
print(rule_between.getRuleSQL())
rules.append(rule_between)
# engine.test_rule(base_query=query, tdq_rule=rule_between, row_limit=20000)
# endregion

# region IN
rule_in = rule_definitions.check_IN(column_name="room_count", values=[1, 3, 4])
print(rule_in.getRuleSQL())
rules.append(rule_in)
# engine.test_rule(base_query=query, tdq_rule=rule_in, row_limit=20000)
# endregion

# region IN
rule_not_in = rule_definitions.check_NOT_IN(column_name="room_count", values=[1, 3, 4])
print(rule_not_in.getRuleSQL())
rules.append(rule_not_in)
# engine.test_rule(base_query=query, tdq_rule=rule_not_in, row_limit=20000)
# endregion

# region IS_TRUE
rule_is_true = rule_definitions.check_IS_TRUE(column_name="vil_valid")
print(rule_is_true.getRuleSQL())
rules.append(rule_is_true)
# engine.test_rule(base_query=query, tdq_rule=rule_is_true, row_limit=20000)
# endregion

# region IS_FALSE
rule_is_false = rule_definitions.check_IS_FALSE(column_name="vil_valid")
print(rule_is_false.getRuleSQL())
rules.append(rule_is_false)
# engine.test_rule(base_query=query, tdq_rule=rule_is_false, row_limit=20000)
# endregion

# region STRING_CONTAINS
rule_string_contains = rule_definitions.check_STRING_CONTAINS(column_name="application_id", search_value="WHITELABEL", case_sensitive=False)
print(rule_string_contains.getRuleSQL())
rules.append(rule_string_contains)
# engine.test_rule(base_query=query, tdq_rule=rule_string_contains)
# endregion

engine.run_data_quality_checks(base_query=query, tdq_rules=rules)
