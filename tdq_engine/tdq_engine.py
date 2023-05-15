class TDQEngine:
    import pandas as pd
    from rule_definitions.tdq_rule_base import TDQRuleBase
    from tdq_engine.tdq_configuration import TDQConfiguration
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

    def _prepare_tdq_base_query(self, query: str) -> dict:
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
        base_uuid = self._get_uuid()
        query_base_cte = f"cte_query_base_{str(base_uuid).replace('-', '_')}"
        if self._is_cte_exists(query):
            pattern = re.compile(r'\)\s*SELECT(?!.*FROM)([\s\S]*)', re.IGNORECASE)
            matches = pattern.search(query)
            if matches:
                base_query = f"{query[:matches.start(0)]}), {query_base_cte} AS (SELECT {query[matches.start(1):]})"
            else:
                raise Exception("Invalid SQL script. Please check the SQL script and try again")
        else:
            base_query = f"WITH {query_base_cte} AS ( {query} )"
        return {"base_uuid": str(base_uuid), "base_query": base_query}

    def _print_prepared_tdq_query(self, tdq_prep_result: dict):
        """
        Prints the details of prepared DQ check block.

                Parameters:
                        dq_prep_result (str): Prepare DQ check information

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

    def _prepare_tdq_query(self, base_query_config: dict, tdq_rules: list[TDQRuleBase]) -> dict:
        """
        Prepare the final SQL query for DQ check by appending DQ check blocks to base query .

                Parameters:
                        base_query (str): Base query preparation result
                        dq_checks (list<dict>) : List of DQ checks that will be applied to base query

                Returns:
                        dq_config (dict) : Final configuration for DQ checks includes base information and DQ checks
        """

        query = base_query_config["base_query"]
        base_uuid = base_query_config["base_uuid"]

        # Append DQ check queries
        query = f"""{query},{','.join(tdq_check.getRuleSQL(base_uuid=base_uuid) for tdq_check in tdq_rules)}"""

        # Prepare final SQL statement
        query = f"""{query} {' UNION ALL '.join(f"SELECT * FROM cte_check_{str(dq_check.getCheckUUID()).replace('-', '_')}" for dq_check in tdq_rules)}"""

        return {"base_uuid": base_query_config["base_uuid"],
                "tdq_check_query": query,
                "tdq_rules": tdq_rules}

    def _prepare_tdq_tables(self) -> dict:
        try:
            from google.cloud import bigquery
            self._log_info("Preparing TDQ tables")

            # If GCP and/or TDQ configuration not defined, raise exception
            if (self._gcp_configuration is None) or (self._tdq_configuration is None):
                raise Exception("Error executing TDQ checks. GCP configuration and TDQ configuration should be defined. Please check the configuration and try again!")

            #region Create TDQ tables
            with bigquery.Client(project=self._gcp_configuration.getProjectId()) as client:

                #region Prepare TDQ summary table
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
                    bigquery.SchemaField(name="execution_end_time", field_type="STRING", description="TDQ data quality check execution end timestamp"),
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
                #endregion

                #region Prepare TDQ results table
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
                #endregion

            #endregion

            self._log_info("TDQ tables created successfully")

            return {"success": True,
                    "project_id": self._gcp_configuration.getProjectId(),
                    "tdq_dataset": self._gcp_configuration.getDatasetId(),
                    "tdq_summary_table": self._gcp_configuration.getTDQSummaryTable(),
                    "tdq_results_table": self._gcp_configuration.getTDQResultsTable()}

        except Exception as ex:
            self._log_error(f"Error creating TDQ tables. Error Message: {str(ex)}")
            return {"success": False, "error": str(ex)}

    def _execute_tdq_checks(self, tdq_check_query: str = None) -> dict:
        try:
            from google.cloud import bigquery

            execution_results = None
            with bigquery.Client(project=self._gcp_configuration.getProjectId()) as client:
                execute_result = client.query(query=tdq_check_query, project=self._gcp_configuration.getProjectId())
                execution_results = execute_result.result()

            if execution_results is not None:
                return {"success": True, "tdq_results": execution_results.to_dataframe()}
            else:
                raise Exception("Error executing TDQ checks")

        except Exception as ex:
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


    # endregion

    # region Public Methods

    def set_TDQConfiguration(self, tdq_configuration: TDQConfiguration = None):
        self._tdq_configuration = tdq_configuration

    def set_GCPConfiguration(self, gcp_configuration: TDQGoogleCloudConfiguration = None):
        self._gcp_configuration = gcp_configuration

    def get_TDQConfiguration(self):
        return self._tdq_configuration

    def get_GCPConfiguration(self):
        return self._gcp_configuration

    def run_data_quality_checks(self,
                                base_query: str,
                                tdq_rules: list[TDQRuleBase] = []):

        import uuid

        # Validate rules
        valid_rules, invalid_rules = self._validate_rules(tdq_rules=tdq_rules)

        # Prepare/Create TDQ tables
        tdq_tables_config = self._prepare_tdq_tables()
        # If success, start preparing and executing TDQ checks
        if tdq_tables_config["success"]:

            # region Prepare data quality checks script

            # Generate unique DQ UUID
            self._log_info("Preparing TDQ base UUID")
            tdq_uuid = uuid.uuid4()
            self._log_info(f"Generated TDQ UUID is {str(tdq_uuid)}")

            # Prepare DQ base config
            self._log_info("Preparing base query config")
            tdq_base_config = self._prepare_tdq_base_query(query=base_query)
            self._log_info(f"Base Query\n{tdq_base_config['base_query'].strip()}")

            # Prepare DQ query
            self._log_info("Preparing TDQ query config (Only for valid rules)")
            tdq_config = self._prepare_tdq_query(base_query_config=tdq_base_config, tdq_rules=valid_rules)
            self._log_info(f"TDQ Checks Query\n{tdq_config['tdq_check_query']}")

            # endregion

            # region Execute TDQ checks script for valid rules

            execution_results = self._execute_tdq_checks(valid_rules)
            print(execution_results["success"])

            # endregion

            # region Execute TDQ checks query and get results as DataFrame

        else:
            # TODO : Failed process
            pass

    # endregion
