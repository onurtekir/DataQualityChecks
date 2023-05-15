import json
import uuid
from numbers import Number
from rule_definitions.tdq_rule_base import TDQRuleBase, RULE_TYPE


class check_NULL(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="NULL",
                         column_name=column_name,
                         threshold=threshold)

    def _prepare_rule_sql(self):
        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{str(self.getRuleType())}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} IS NULL) AS unexpected_count,
                    COUNTIF({self.getColumnName()} IS NOT NULL) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} IS NULL) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} IS NOT NULL) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} IS NULL) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_NOT_NULL(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="NOT_NULL",
                         column_name=column_name,
                         threshold=threshold)

    def _prepare_rule_sql(self):
        query = f"""
                cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                    SELECT 
                        "{str(self.getBaseUUID())}" AS check_uuid,
                        "{str(self.getCheckUUID())}" AS rule_uuid,
                        "{self.getRuleType().value}" AS type,
                        "{self.getRuleCheckType()}" AS check_type,
                        "{self.getColumnName()}" AS column_name,
                        '{json.dumps(self.getParameters())}' AS parameters,
                        {self.getThreshold()} AS threshold,
                        COUNT(1) AS row_count,
                        COUNTIF({self.getColumnName()} IS NULL) AS unexpected_count,
                        COUNTIF({self.getColumnName()} IS NOT NULL) AS expected_count,
                        ROUND((COUNTIF({self.getColumnName()} IS NULL) / COUNT(1)), 4) AS unexpected_ratio,
                        ROUND((COUNTIF({self.getColumnName()} IS NOT NULL) / COUNT(1)), 4) AS expected_ratio,
                        IF((COUNTIF({self.getColumnName()} IS NULL) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                        {self.isValid()} AS is_valid
                    FROM
                        {self.getBaseCTE()}
                )
            """
        return query


class check_NULL_OR_EMPTY(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0, is_trimmed: bool = False):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="NULL_OR_EMPTY",
                         column_name=column_name,
                         threshold=threshold)
        # Set rule specific parameter
        self.setParameter(key="is_trimmed", value=is_trimmed)

    def _prepare_rule_sql(self):

        trimmed = self.getParameter(key="is_trimmed", default=False)

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} IS NOT NULL AND {f"TRIM({self.getColumnName()}) != ''" if trimmed else f"{self.getColumnName()} != ''"}) AS unexpected_count,
                    COUNTIF({self.getColumnName()} IS NULL OR {f"TRIM({self.getColumnName()}) = ''" if trimmed else f"{self.getColumnName()} = ''"}) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} IS NOT NULL AND {f"TRIM({self.getColumnName()}) != ''" if trimmed else f"{self.getColumnName()} != ''"}) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} IS NULL AND {f"TRIM({self.getColumnName()}) = ''" if trimmed else f"{self.getColumnName()} = ''"}) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} IS NOT NULL AND {f"TRIM({self.getColumnName()}) != ''" if trimmed else f"{self.getColumnName()} != ''"}) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_GREATER_THAN(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0, value: Number = None, or_equal: bool = False):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="GREATER_THAN",
                         column_name=column_name,
                         threshold=threshold)
        # Set rule specific parameters
        self.setParameter(key="value", value=value)
        self.setParameter(key="or_equal", value=or_equal)

    def _prepare_rule_sql(self):

        # Get rule specific parameters
        value = self.getParameter(key="value", default=None)
        or_equal = self.getParameter(key="or_equal", default=False)

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} {"<" if or_equal else "<="} {value}) AS unexpected_count,
                    COUNTIF({self.getColumnName()} {">=" if or_equal else ">"} {value}) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} {"<" if or_equal else "<="} {value}) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} {">=" if or_equal else ">"} {value}) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} {"<" if or_equal else "<="} {value}) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_LESS_THAN(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0, value: Number = None, or_equal: bool = False):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="LESS_THAN",
                         column_name=column_name,
                         threshold=threshold)
        # Set rule specific parameters
        self.setParameter(key="value", value=value)
        self.setParameter(key="or_equal", value=or_equal)

    def _prepare_rule_sql(self):

        # Get rule specific parameters
        value = self.getParameter(key="value", default=None)
        or_equal = self.getParameter(key="or_equal", default=False)

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} {">" if or_equal else ">="} {value}) AS unexpected_count,
                    COUNTIF({self.getColumnName()} {"<=" if or_equal else "<"} {value}) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} {">" if or_equal else ">="} {value}) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} {"<=" if or_equal else "<"} {value}) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} {">" if or_equal else ">="} {value}) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """


class check_BETWEEN(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0,
                 min_value: Number = None, max_value: Number = None, strict_min: bool = False, strict_max:bool = False):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="BETWEEN",
                         column_name=column_name,
                         threshold=threshold)
        # Set rule specific parameters
        self.setParameter(key="min_value", value=min_value)
        self.setParameter(key="max_value", value=max_value)
        self.setParameter(key="strict_min", value=strict_min)
        self.setParameter(key="strict_max", value=strict_max)

    def _prepare_rule_sql(self):

        # Get rule specific parameters
        min_value = self.getParameter(key="min_value", default=None)
        max_value = self.getParameter(key="max_value", default=None)
        strict_min = self.getParameter(key="strict_min", default=False)
        strict_max = self.getParameter(key="strict_max", default=False)

        if min_value is None and max_value is None:
            raise Exception("At least min_value and/or max_value should be defined. Please check your parameters and try again")

        expected_between_sql_parts = []
        unexpected_between_sql_parts = []

        if min_value is not None:
            # Prepare min sql parts
            expected_between_sql_parts.append(f"{self.getColumnName()} >= {min_value}" if strict_min else f"{self.getColumnName()} > {min_value}")
            unexpected_between_sql_parts.append(f"{self.getColumnName()} < {min_value}" if strict_min else f"{self.getColumnName()} <= {min_value}")

        if max_value is not None:
            # Prepare max sql parts
            expected_between_sql_parts.append(f"{self.getColumnName()} <= {max_value}" if strict_max else f"{self.getColumnName()} < {max_value}")
            unexpected_between_sql_parts.append(f"{self.getColumnName()} > {max_value}" if strict_max else f"{self.getColumnName()} >= {max_value}")

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    {f"COUNTIF({' OR '.join(unexpected_between_sql_parts)})"} AS unexpected_count,
                    {f"COUNTIF({' AND '.join(expected_between_sql_parts)})"} AS expected_count
                    ROUND(({f"COUNTIF({' OR '.join(unexpected_between_sql_parts)})"} / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND(({f"COUNTIF({' AND '.join(expected_between_sql_parts)})"} / COUNT(1)), 4) AS expected_ratio,
                    IF(({f"COUNTIF({' OR '.join(unexpected_between_sql_parts)})"} / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_IN(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0, values: list = []):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="IN",
                         column_name=column_name,
                         threshold=threshold)

        # Get rule specific parameter
        self.setParameter(key="values", value=values)

    def _prepare_rule_sql(self):

        # Get rule specific parameter
        values = self.getParameter(key="values", default=[])

        if values is None or len(values) == 0:
            raise Exception("Values should not be NULL or EMPTY. Please define values and try again")

        # Prepare IN SQL statement
        in_sql_statement = ""
        if isinstance(values[0], Number) or isinstance(values[0], bool):
            # Boolean or Number
            in_sql_statement = ','.join(str(x) for x in values)
        else:
            # String
            in_sql_statement = ','.join(f"'{x}'" for x in values)

        for value in values:
            if isinstance(value, Number):
                ','.join(str(x) for x in values)

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} NOT IN({in_sql_statement})) AS unexpected_count,
                    COUNTIF({self.getColumnName()} IN({in_sql_statement})) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} NOT IN({in_sql_statement})) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} IN({in_sql_statement})) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} NOT IN({in_sql_statement})) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_NOT_IN(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0, values: list = []):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="NOT_IN",
                         column_name=column_name,
                         threshold=threshold)

        # Get rule specific parameter
        self.setParameter(key="values", value=values)

    def _prepare_rule_sql(self):

        # Get rule specific parameter
        values = self.getParameter(key="values", default=[])

        if values is None or len(values) == 0:
            raise Exception("Values should not be NULL or EMPTY. Please define values and try again")

        # Prepare IN SQL statement
        in_sql_statement = ""
        if isinstance(values[0], Number) or isinstance(values[0], bool):
            # Boolean or Number
            in_sql_statement = ','.join(str(x) for x in values)
        else:
            # String
            in_sql_statement = ','.join(f"'{x}'" for x in values)

        for value in values:
            if isinstance(value, Number):
                ','.join(str(x) for x in values)

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} IN({in_sql_statement})) AS unexpected_count,
                    COUNTIF({self.getColumnName()} NOT IN({in_sql_statement})) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} IN({in_sql_statement})) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} NOT IN({in_sql_statement})) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} IN({in_sql_statement})) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_IS_TRUE(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="IS_TRUE",
                         column_name=column_name,
                         threshold=threshold)

    def _prepare_rule_sql(self):

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} IS NOT TRUE) AS unexpected_count,
                    COUNTIF({self.getColumnName()} IS TRUE) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} IS NOT TRUE) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} IS TRUE) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} IS NOT TRUE) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_IS_FALSE(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="IS_FALSE",
                         column_name=column_name,
                         threshold=threshold)

    def _prepare_rule_sql(self):

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF({self.getColumnName()} IS NOT FALSE) AS unexpected_count,
                    COUNTIF({self.getColumnName()} IS FALSE) AS expected_count,
                    ROUND((COUNTIF({self.getColumnName()} IS NOT FALSE) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF({self.getColumnName()} IS FALSE) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF({self.getColumnName()} IS NOT FALSE) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query


class check_STRING_CONTAINS(TDQRuleBase):

    def __init__(self, column_name: str = None, threshold: float = 0.0,
                 search_value: str = None, case_sensitive: bool = True):
        super().__init__(rule_type=RULE_TYPE.ROW_BASED,
                         rule_check_type="STRING_CONTAINS",
                         column_name=column_name,
                         threshold=threshold)

        # Set rule specific parameters
        self.setParameter(key="search_value", value=search_value)
        self.setParameter(key="case_sensitive", value=case_sensitive)


    def _prepare_rule_sql(self):

        # Get rule specific parameters
        search_value = self.getParameter(key="search_value", default=None)
        case_sensitive = self.getParameter(key="case_sensitive", default=True)

        # Prepare search SQL statement
        sql_statement = search_value if case_sensitive else f"(?i){search_value}"

        query = f"""
            cte_check_{str(self.getCheckUUID()).replace('-', '_')} AS (
                SELECT 
                    "{str(self.getBaseUUID())}" AS check_uuid,
                    "{str(self.getCheckUUID())}" AS rule_uuid,
                    "{self.getRuleType().value}" AS type,
                    "{self.getRuleCheckType()}" AS check_type,
                    "{self.getColumnName()}" AS column_name,
                    '{json.dumps(self.getParameters())}' AS parameters,
                    {self.getThreshold()} AS threshold,
                    COUNT(1) AS row_count,
                    COUNTIF(NOT REGEXP_CONTAINS({self.getColumnName()}, r'{sql_statement}')) AS unexpected_count,
                    COUNTIF(REGEXP_CONTAINS({self.getColumnName()}, r'{sql_statement}')) AS expected_count,
                    ROUND((COUNTIF(NOT REGEXP_CONTAINS({self.getColumnName()}, r'{sql_statement}')) / COUNT(1)), 4) AS unexpected_ratio,
                    ROUND((COUNTIF(REGEXP_CONTAINS({self.getColumnName()}, r'{sql_statement}')) / COUNT(1)), 4) AS expected_ratio,
                    IF((COUNTIF(NOT REGEXP_CONTAINS({self.getColumnName()}, r'{sql_statement}')) / COUNT(1)) > {self.getThreshold()}, False, True) AS is_passed,
                    {self.isValid()} AS is_valid
                FROM
                    {self.getBaseCTE()}
            )
        """
        return query





