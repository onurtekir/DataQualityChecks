class TDQEngine:
    from rule_definitions.tdq_rule_base import TDQRuleBase
    from tdq_engine.tdq_configuration import TDQConfiguration
    from tdq_engine.tdq_result import TDQResult
    from tdq_engine.tdq_google_cloud_configuration import TDQGoogleCloudConfiguration

    def __init__(self, dq_check_configuration: TDQConfiguration = None, gcp_configuration: TDQGoogleCloudConfiguration = None):
        self._tdq_configuration = dq_check_configuration
        self._gcp_configuration = gcp_configuration

    # region Private Methods

    def _get_printable_log_time(self):
        from datetime import datetime
        return datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    def _log_info(self, message: str = ""):
        print(f"{self._get_printable_log_time()} [INFO] : {message}")

    def _log_error(self, message: str = ""):
        print(f"{self._get_printable_log_time()} [ERROR] : {message}")

    def _log_warn(self, message: str = ""):
        print(f"{self._get_printable_log_time()} [WARN] : {message}")

    def _get_uuid(self) -> any:
        import uuid
        return uuid.uuid4()

    def _is_cte_exists(self, query: str) -> bool:
        """
        Analyse the SQL script and returns True if query contains CTE blocks.

                Parameters:
                        query (str): SQL query of the data source

                Returns:
                        True/False
        """

        import re
        # Define regex pattern for CTE blocks
        cte_pattern = re.compile(r'\bWITH\b\s+\b\w+\b\s+AS\s+\(', re.IGNORECASE)

        # Find all matches of the pattern in the SQL script
        cte_blocks = cte_pattern.findall(query)

        return True if len(cte_blocks) == 1 else False

    def _prepare_tdq_base_query(self, query: str, row_limit: int = None) -> dict:
        """
        Analyse and prepare DQ base script using the query of the data source.

                Parameters:
                        query (str): SQL query of the data source

                Returns:
                        base_query_result (dict)
                            base_uuid (str) : Base DQ UUID. This UUID will be used to define DQ check session
                            base_query (str) : Prepared base query of the DQ SQL script
        """

        import re
        base_query = ""
        check_uuid = self._get_uuid()
        query_base_cte = f"cte_query_base_{str(check_uuid).replace('-', '_')}"
        if self._is_cte_exists(query):
            pattern = re.compile(r'\)\s*SELECT(?!.*FROM)([\s\S]*)', re.IGNORECASE)
            matches = pattern.search(query)
            if matches:
                base_query = f"{query[:matches.start(0)]}), {query_base_cte} AS (SELECT {query[matches.start(1):]} {f'LIMIT {row_limit}' if row_limit is not None else ''})"
            else:
                raise Exception("Invalid SQL script. Please check the SQL script and try again")
        else:
            base_query = f"WITH {query_base_cte} AS ( {query} )"
        return {"check_uuid": str(check_uuid), "base_query": base_query}

    def _print_prepared_tdq_query(self, tdq_prep_result: dict):
        """
        Prints the details of prepared DQ check block.

                Parameters:
                        tdq_prep_result (str): Prepare DQ check information

        """
        print(f"Check UUID: {tdq_prep_result['check_uuid']}")
        print(f"Type: {tdq_prep_result['type']}")
        print(f"Check Type: {tdq_prep_result['check_type']}")
        print(f"Column Name: {tdq_prep_result['column_name']}")
        if "params" in tdq_prep_result:
            print("Parameters")
            for param in tdq_prep_result["params"]:
                print(f"\t{param}: {tdq_prep_result['params'][param]}")
        print(f"Threshold: {tdq_prep_result['threshold']}")
        print(f"QUERY")
        print(tdq_prep_result["check_query"])

    def _prepare_tdq_check_query_config(self, base_query_config: dict, tdq_rules: list[TDQRuleBase]) -> dict:
        """
        Prepare the final SQL query for DQ check by appending DQ check blocks to base query .

                Parameters:
                        base_query_config (dict): Base query configuration
                        tdq_rules (list<dict>) : List of DQ checks that will be applied to base query

                Returns:
                        dq_config (dict) : Final configuration for DQ checks includes base information and DQ checks
        """

        query = base_query_config["base_query"]
        check_uuid = base_query_config["check_uuid"]

        # Append DQ check queries
        query = f"""{query},{','.join(tdq_check.getRuleSQL(check_uuid=check_uuid) for tdq_check in tdq_rules)}"""

        # Prepare final SQL statement
        query = f"""{query} {' UNION ALL '.join(f"SELECT * FROM cte_check_{str(dq_check.getRuleCheckUUID()).replace('-', '_')}" for dq_check in tdq_rules)}"""

        return {"base_uuid": base_query_config["check_uuid"],
                "tdq_check_query": query,
                "tdq_rules": tdq_rules}

    def _prepare_tdq_tables(self) -> dict:
        try:
            from google.cloud import bigquery
            self._log_info("Preparing TDQ tables")

            # If GCP and/or TDQ configuration not defined, raise exception
            if (self._gcp_configuration is None) or (self._tdq_configuration is None):
                raise Exception("Error executing TDQ checks. GCP configuration and TDQ configuration should be defined. Please check the configuration and try again!")

            # region Create TDQ tables
            with bigquery.Client(project=self._gcp_configuration.getProjectId()) as client:

                # region Prepare TDQ summary table
                self._log_info("Preparing TDQ summary schema")
                schema = [
                    bigquery.SchemaField(name="check_date", field_type="DATE", description="TDQ data quality check date"),
                    bigquery.SchemaField(name="check_uuid", field_type="STRING", description="TDQ data quality check unique UUID"),
                    bigquery.SchemaField(name="check_name", field_type="STRING", description="TDQ data quality check name"),
                    bigquery.SchemaField(name="check_description", field_type="STRING", description="TDQ data quality check description"),
                    bigquery.SchemaField(name="rule_count", field_type="INT64", description="TDQ expectations count"),
                    bigquery.SchemaField(name="valid_rule_count", field_type="INT64", description="TDQ valid expectations count"),
                    bigquery.SchemaField(name="invalid_rule_count", field_type="INT64", description="TDQ invalid expectations count"),
                    bigquery.SchemaField(name="success_count", field_type="INT64", description="TDQ success expectations count"),
                    bigquery.SchemaField(name="failed_count", field_type="INT64", description="TDQ failed expectations count"),
                    bigquery.SchemaField(name="execution_start_time", field_type="TIMESTAMP", description="TDQ data quality check execution start timestamp"),
                    bigquery.SchemaField(name="execution_end_time", field_type="TIMESTAMP", description="TDQ data quality check execution end timestamp"),
                    bigquery.SchemaField(name="is_success", field_type="BOOLEAN", description="TDQ data quality check success/failed flag"),
                    bigquery.SchemaField(name="execution_parameters", field_type="JSON", description="TDQ data quality check execution parameters"),
                    bigquery.SchemaField(name="execution_message", field_type="STRING", description="TDQ data quality check execution message.")
                ]
                # Prepare dataset reference
                dataset_ref = bigquery.DatasetReference(project=self.get_GCPConfiguration().getProjectId(), dataset_id=self._gcp_configuration.getDatasetId())
                # Prepare summary table reference
                table_ref = bigquery.TableReference(table_id=self._gcp_configuration.getTDQSummaryTable(), dataset_ref=dataset_ref)
                table = bigquery.Table(table_ref=table_ref, schema=schema)
                # Prapare partitioning
                table_partition = bigquery.table.TimePartitioning(type_=bigquery.table.TimePartitioningType.DAY, field="check_date")
                table.time_partitioning = table_partition
                # Create table if not exists
                self._log_info(f"Creating TDQ summary table `{self._gcp_configuration.getProjectId()}`.`{self._gcp_configuration.getDatasetId()}.{self._gcp_configuration.getTDQSummaryTable()}`")
                client.create_table(table=table, exists_ok=True)
                # endregion

                # region Prepare TDQ results table
                schema = [
                    bigquery.SchemaField(name="check_date", field_type="DATE", description="TDQ check date"),
                    bigquery.SchemaField(name="check_uuid", field_type="STRING", description="TDQ check unique UUID"),
                    bigquery.SchemaField(name="rule_uuid", field_type="STRING", description="TDQ rule unique UUID"),
                    bigquery.SchemaField(name="type", field_type="STRING", description="TDQ type"),
                    bigquery.SchemaField(name="check_type", field_type="STRING", description="TDQ rule check type"),
                    bigquery.SchemaField(name="column_name", field_type="STRING", description="TDQ rule check column name"),
                    bigquery.SchemaField(name="parameters", field_type="JSON", description="TDQ rule check parameters"),
                    bigquery.SchemaField(name="threshold", field_type="FLOAT64", description="TDQ rule check validation threshold"),
                    bigquery.SchemaField(name="row_count", field_type="INT64", description="Total rows"),
                    bigquery.SchemaField(name="unexpected_count", field_type="INT64", description="Unexpected rows count"),
                    bigquery.SchemaField(name="expected_count", field_type="INT64", description="Expected rows count"),
                    bigquery.SchemaField(name="unexpected_ratio", field_type="FLOAT64", description="Unexpected results ratio"),
                    bigquery.SchemaField(name="expected_ratio", field_type="FLOAT64", description="Expected results ratio"),
                    bigquery.SchemaField(name="is_valid", field_type="BOOLEAN", description="True if rule check definition is valid"),
                    bigquery.SchemaField(name="is_passed", field_type="BOOLEAN", description="True if unexpected ratio is less than threshold else False")
                ]
                # Prepare table reference
                table_ref = bigquery.TableReference(table_id=self._gcp_configuration.getTDQResultsTable(), dataset_ref=dataset_ref)
                table = bigquery.Table(table_ref=table_ref, schema=schema)
                # Prepare partitioning
                table_partition = bigquery.table.TimePartitioning(type_=bigquery.table.TimePartitioningType.DAY, field="check_date")
                table.time_partitioning = table_partition
                # Create table if not exists
                self._log_info(f"Creating TDQ results table `{self._gcp_configuration.getProjectId()}`.`{self._gcp_configuration.getDatasetId()}.{self._gcp_configuration.getTDQResultsTable()}`")
                client.create_table(table=table, exists_ok=True)
                # endregion

            # endregion

            self._log_info("TDQ tables created successfully")

            return {"success": True,
                    "project_id": self._gcp_configuration.getProjectId(),
                    "tdq_dataset": self._gcp_configuration.getDatasetId(),
                    "tdq_summary_table": self._gcp_configuration.getTDQSummaryTable(),
                    "tdq_results_table": self._gcp_configuration.getTDQResultsTable()}

        except Exception as ex:
            self._log_error(f"Error creating TDQ tables. Error Message: {str(ex)}")
            return {"success": False, "error": str(ex)}

    def _execute_bq_query(self, project_id: str = None, query: str = None) -> dict:
        try:
            from google.cloud import bigquery

            execution_results = None
            with bigquery.Client(project=self._gcp_configuration.getProjectId()) as client:
                query_job = client.query(query=query, project=project_id)
                execution_results = query_job.result().to_dataframe()

            if execution_results is not None:
                return {"success": True, "results": execution_results}
            else:
                raise Exception("Error executing BigQuery script")

        except Exception as ex:
            self._log_error(f"Error execution BigQuery script. Error message: {str(ex)}")
            return {"success": False, "error": str(ex)}

    def _validate_rules(self, tdq_rules: list[TDQRuleBase]):

        valid_rules = []
        invalid_rules = []

        for rule in tdq_rules:
            if rule.isValid():
                valid_rules.append(rule)
            else:
                invalid_rules.append(rule)

        return valid_rules, invalid_rules

    def _generate_invalid_checks_dataset(self, invalid_rules: list[TDQRuleBase] = []):
        import pandas as pd
        import json

        df = pd.DataFrame(columns=["check_uuid",
                                   "rule_uuid",
                                   "type",
                                   "check_type",
                                   "column_name",
                                   "parameters",
                                   "threshold",
                                   "row_count",
                                   "unexpected_count",
                                   "expected_count",
                                   "unexpected_ratio",
                                   "expected_ratio",
                                   "is_passed",
                                   "is_valid"])
        for invalid_rule in invalid_rules:
            df = pd.concat([df, pd.DataFrame({
                "check_uuid": [str(invalid_rule.getCheckUUID())],
                "rule_uuid": [str(invalid_rule.getRuleCheckUUID())],
                "type": [invalid_rule.getRuleType().value],
                "check_type": [invalid_rule.getRuleCheckType()],
                "column_name": [invalid_rule.getColumnName()],
                "parameters": [json.dumps(invalid_rule.getParameters())],
                "threshold": [invalid_rule.getThreshold()],
                "row_count": [None],
                "unexpected_count": [None],
                "expected_count": [None],
                "unexpected_ratio": [None],
                "expected_ratio": [None],
                "is_passed": [False],
                "is_valid": [False]
            })], ignore_index=True)

        return df

    def _save_summary(self, tdq_result: TDQResult = None) -> dict:
        try:
            from google.cloud import bigquery
            with bigquery.Client(project=tdq_result.getGCPProjectId()) as client:
                table_id = f"{tdq_result.getGCPProjectId()}.{tdq_result.getGCPDatasetId()}.{tdq_result.getGCPSummaryTable()}"
                table = client.get_table(table=table_id)
                df_summary_row = tdq_result.getSummaryDataFrame()
                errors = client.insert_rows_from_dataframe(table=table, dataframe=df_summary_row)
                if not any(errors):
                    self._log_info(f"TDQ summary results saved to `{tdq_result.getGCPProjectId()}`.`{tdq_result.getGCPDatasetId()}.{tdq_result.getGCPSummaryTable()}` successfully")
                    return {"success": True}
                else:
                    return {"success": False, "error": errors}
        except Exception as ex:
            self._log_error(f"Error saving TDQ summary. Error message: {str(ex)}")
            return {"success": False, "error": [[str(ex)]]}

    def _save_tdq_check_results(self, tdq_result: TDQResult = None) -> dict:
        try:
            from google.cloud import bigquery
            with bigquery.Client(project=tdq_result.getGCPProjectId()) as client:
                table_id = f"{tdq_result.getGCPProjectId()}.{tdq_result.getGCPDatasetId()}.{tdq_result.getGCPResultsTable()}"
                table = client.get_table(table=table_id)
                df_check_results = tdq_result.getResultsDataFrame()
                errors = client.insert_rows_from_dataframe(table=table, dataframe=df_check_results)
                if not any(errors):
                    self._log_info(f"TDQ check results saved to `{tdq_result.getGCPProjectId()}`.`{tdq_result.getGCPDatasetId()}.{tdq_result.getGCPResultsTable()}` successfully")
                    return {"success": True}
                else:
                    return {"success": False, "error": errors}

        except Exception as ex:
            self._log_error(f"Error saving TDQ check results. Error message: {str(ex)}")
            return {"success": False, "error": [[str(ex)]]}

    # endregion

    # region Public Methods

    def _execute_tdq_checks(self, base_query: str, tdq_rules: list[TDQRuleBase] = [], row_limit: int = None):

        # Import libraries
        import pandas as pd

        # Validate rules
        valid_rules, invalid_rules = self._validate_rules(tdq_rules=tdq_rules)

        # Prepare/Create TDQ tables
        tdq_tables_config = self._prepare_tdq_tables()

        # If success, start preparing and executing TDQ checks
        if tdq_tables_config["success"]:

            # Prepare TDQ base config
            self._log_info("Preparing TDQ base query config")
            tdq_base_config = self._prepare_tdq_base_query(query=base_query, row_limit=row_limit)
            check_uuid = tdq_base_config['check_uuid']
            self._log_info(f"TDQ Check UUID: {check_uuid}")
            self._log_info(f"TDQ Base Query\n{tdq_base_config['base_query'].strip()}")

            # Prepare TDQ checks query for valid rules
            self._log_info("Preparing TDQ query config (Only for valid rules)")
            tdq_query_config = self._prepare_tdq_check_query_config(base_query_config=tdq_base_config, tdq_rules=valid_rules)
            self._log_info(f"TDQ Checks Query\n{tdq_query_config['tdq_check_query']}")

            # Execute TDQ checks query
            self._log_info(f"Start executing valid TDQ checks. {len(valid_rules)} will be executed!")
            execution_results = self._execute_bq_query(project_id=self.get_GCPConfiguration().getProjectId(), query=tdq_query_config['tdq_check_query'])

            # If success, append invalid rules information to results
            if execution_results["success"]:
                # Get valid checks execution results dataframe
                df_tdq_results = execution_results["results"]
                self._log_info("Valid TDQ checks execution completed successfully")
                self._log_info("Adding invalid TDQ checks with is_valid=False flag")

                for invalid_rule in invalid_rules:
                    invalid_rule.setCheckUUID(check_uuid)
                    df_tdq_results = pd.concat([df_tdq_results, self._generate_invalid_checks_dataset(invalid_rules=[invalid_rule])], ignore_index=True)

                return {"success": True, "check_uuid": check_uuid, "tdq_results": df_tdq_results}

            else:
                return {"success": False, "check_uuid": check_uuid, "error": execution_results.get("error", "Unknown error")}

            # endregion

            # region Execute TDQ checks query and get results as DataFrame

        else:
            return {"success": False, "error": tdq_tables_config.get("error", "Unknown error")}

    def set_GCPConfiguration(self, gcp_configuration: TDQGoogleCloudConfiguration = None):
        self._gcp_configuration = gcp_configuration

    def set_TDQConfiguration(self, tdq_configuration: TDQConfiguration = None):
        self._tdq_configuration = tdq_configuration

    def get_TDQConfiguration(self):
        return self._tdq_configuration

    def get_GCPConfiguration(self):
        return self._gcp_configuration

    def run_data_quality_checks(self, base_query: str, tdq_rules: list[TDQRuleBase] = [], save_results: bool = True):
        from datetime import datetime
        from tdq_engine.tdq_result import TDQResult

        start_time = datetime.utcnow()
        df_tdq_results = self._execute_tdq_checks(base_query=base_query, tdq_rules=tdq_rules)
        end_time = datetime.utcnow()
        if df_tdq_results["success"]:
            tdq_results = TDQResult()
            tdq_results.setCheckUUID(df_tdq_results["check_uuid"])
            tdq_results.setCheckName(self.get_TDQConfiguration().getTDQCheckName())
            tdq_results.setCheckDescription(self.get_TDQConfiguration().getTDQCheckDescription())
            tdq_results.setCheckParameters(self.get_TDQConfiguration().getTDQParameters())
            tdq_results.setGCPProjectId(self.get_GCPConfiguration().getProjectId())
            tdq_results.setGCPDatasetId(self.get_GCPConfiguration().getDatasetId())
            tdq_results.setGCPResultsTable(self.get_GCPConfiguration().getTDQResultsTable())
            tdq_results.setGCPSummaryTable(self.get_GCPConfiguration().getTDQSummaryTable())
            tdq_results.processCheckResults(df_tdq_results["tdq_results"])
            tdq_results.setStartTime(start_time=start_time)
            tdq_results.setEndTime(end_time=end_time)
            self._log_info(f"TDQ checks execution finished in {tdq_results.getDurationSeconds()} seconds!")

            if save_results:
                self._log_info(f"Saving TDQ checks summary to `{self.get_GCPConfiguration().getProjectId()}`.`{self.get_GCPConfiguration().getDatasetId()}.{self.get_GCPConfiguration().getTDQSummaryTable()}`")
                summary_save_result = self._save_summary(tdq_result=tdq_results)
                if summary_save_result["success"]:
                    self._log_info(f"Saving TDQ check results summary to `{self.get_GCPConfiguration().getProjectId()}`.`{self.get_GCPConfiguration().getDatasetId()}.{self.get_GCPConfiguration().getTDQResultsTable()}`")
                    check_results_save_result = self._save_tdq_check_results(tdq_result=tdq_results)
                    if check_results_save_result["success"]:
                        return {"success": True, "tdq_results": tdq_results}
                    else:
                        return check_results_save_result
                else:
                    return summary_save_result
        else:
            return {"success": False, "error": df_tdq_results.get("error", "Unknown error")}

    def test_rule(self, base_query: str, tdq_rule: TDQRuleBase = None, row_limit: int = None):
        import json

        # Check if rule is valid. If the rule is invalid return False with `Rule is not valid` error
        if not tdq_rule.isValid():
            return {"success": False, "error": "Rule is not valid. Please check the rule and try again"}

        df_tdq_test_rule_results = self._execute_tdq_checks(base_query=base_query, tdq_rules=[tdq_rule], row_limit=row_limit)
        if df_tdq_test_rule_results["success"]:
            df_results = df_tdq_test_rule_results["tdq_results"]
            df_results = df_results.drop(columns=['check_uuid', 'rule_uuid'], axis=1)

            # Print Test Results
            print("#### TDQ Rule Check Test Results ####")
            print(f"Rule Type        : {df_results.iloc[0]['type']}")
            print(f"Rule Check Type  : {df_results.iloc[0]['check_type']}")
            print(f"Column Name      : {df_results.iloc[0]['column_name']}")
            parameters = json.loads(df_results.iloc[0]['parameters'])
            if len(parameters) > 0:
                print("Parameters")
                for parameter in parameters:
                    print(f"\t- {parameter}: {parameters[parameter]}")
            else:
                print("Parameters: No parameters specified")
            print(f"Threshold        : {'{0:.0%}'.format(df_results.iloc[0]['threshold'])}")
            print(f"Passed           : {df_results.iloc[0]['is_passed']}")

            # Print statistics if valid
            print(f"Row Count        : {df_results.iloc[0]['row_count']}")
            print(f"Expected Count   : {'{0:.0%}'.format(df_results.iloc[0]['expected_ratio'])}\t{df_results.iloc[0]['expected_count']}")
            print(f"Unexpected Count : {'{0:.0%}'.format(df_results.iloc[0]['unexpected_ratio'])}\t{df_results.iloc[0]['unexpected_count']}")

            return {"success": True, "test_results": df_results}
        else:
            return {"success": False, "error": df_tdq_test_rule_results.get("error", "Unknown error")}

    # endregion
