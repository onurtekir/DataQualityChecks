from rule_definitions.tdq_rule_base import RULE_TYPE
from datetime import datetime
import uuid

class TDQResultItem:

    def __init__(self):
        self._result_row = None
        self._check_uuid = None
        self._rule_uuid = None
        self._rule_type = None
        self._check_type = None
        self._column_name = None
        self._parameters = None
        self._threshold = None
        self._row_count = None
        self._unexpected_count = None
        self._expected_count = None
        self._unexpected_ratio = None
        self._expected_ratio = None
        self._is_valid = None
        self._is_passed = None

    def process_result_row(self, result_row: any = None) -> bool:
        import json
        import pandas as pd
        if isinstance(result_row, pd.Series):
            self._result_row = result_row
            self._check_uuid = result_row.get(key="check_uuid", default=None)
            self._rule_uuid = result_row.get(key="rule_uuid", default=None)
            self._rule_type = RULE_TYPE(value=result_row.get(key="type", default=None))
            self._check_type = result_row.get(key="check_type", default=None)
            self._column_name = result_row.get(key="column_name", default=None)
            self._parameters = json.loads(result_row.get(key="parameters", default='{}'))
            self._threshold = result_row.get(key="threshold", default=0.0)
            self._row_count = result_row.get(key="row_count", default=0)
            self._unexpected_count = result_row.get(key="unexpected_count", default=0)
            self._expected_count = result_row.get(key="expected_count", default=0)
            self._unexpected_ratio = result_row.get(key="unexpected_ratio", default=0.0)
            self._expected_ratio = result_row.get(key="expected_ratio", default=0.0)
            self._is_valid = result_row.get(key="is_valid", default=False)
            self._is_passed = result_row.get(key="is_passed", default=False)
            return True
        else:
            return False

    def getCheckUUID(self):
        return self._check_uuid

    def getRuleCheckUUID(self):
        return self._rule_uuid

    def getRuleType(self) -> RULE_TYPE:
        return self._rule_type if self._rule_type is not None else None

    def getCheckType(self):
        return self._check_type

    def getColumnName(self):
        return self._column_name

    def getParameters(self):
        return self._parameters

    def getThreshold(self):
        return self._threshold

    def getRowCount(self):
        return self._row_count

    def getUnexpectedCount(self):
        return self._unexpected_count

    def getExpectedCount(self):
        return self._expected_count

    def getUnexpectedRatio(self):
        return self._unexpected_ratio

    def getExpectedRatio(self):
        return self._expected_ratio

    def isValid(self):
        return self._is_valid

    def isPassed(self):
        return self._is_passed

    def toDataFrame(self):
        import json
        import pandas as pd
        return pd.DataFrame({
            "check_uuid": [str(self.getCheckUUID())],
            "rule_uuid": [str(self.getRuleCheckUUID())],
            "type": [self.getRuleType().value] if self.getRuleType() is not None else ["UNDEFINED"],
            "check_type": [self.getCheckType()],
            "column_name": [self.getColumnName()],
            "parameters": [json.dumps(self.getParameters())] if self.getParameters() is not None else [json.dumps({})],
            "threshold": [self.getThreshold()],
            "row_count": [self.getRowCount()],
            "unexpected_count": [self.getUnexpectedCount()],
            "expected_count": [self.getExpectedCount()],
            "unexpected_ratio": [self.getUnexpectedRatio()],
            "expected_ratio": [self.getExpectedRatio()],
            "is_passed": [self.isPassed()],
            "is_valid": [self.isValid()]
        })


class TDQResult:

    def __init__(self):
        self._checkUUID = None
        self._tdqResultItems = []
        self._tdqCheckName = None
        self._tdqCheckDescription = None
        self._tdqCheckParameters = None
        self._gcpProjectId = None
        self._gcpDatasetId = None
        self._gcpResultsTable = None
        self._gcpSummaryTable = None
        self._tdqStartTime = None
        self._tdqEndTime = None

    def setCheckUUID(self, check_uuid: uuid.UUID):
        self._checkUUID = check_uuid

    def setCheckName(self, check_name: str):
        self._tdqCheckName = check_name

    def setCheckDescription(self, check_description: str):
        self._tdqCheckDescription = check_description

    def setCheckParameters(self, check_parameters: dict):
        self._tdqCheckParameters = check_parameters

    def setGCPProjectId(self, project_id: str):
        self._gcpProjectId = project_id

    def setGCPDatasetId(self, dataset_id: str):
        self._gcpDatasetId = dataset_id

    def setGCPResultsTable(self, results_table: str):
        self._gcpResultsTable = results_table

    def setGCPSummaryTable(self, summary_table: str):
        self._gcpSummaryTable = summary_table

    def setStartTime(self, start_time: datetime = None):
        self._tdqStartTime = start_time

    def setEndTime(self, end_time: datetime = None):
        self._tdqEndTime = end_time

    def processCheckResults(self, results_dataframe: any = None):
        import pandas as pd
        if (results_dataframe is not None) and (isinstance(results_dataframe, pd.DataFrame)):
            for result_item_index in range(len(results_dataframe)):
                result_item = TDQResultItem()
                if result_item.process_result_row(result_row=results_dataframe.iloc[result_item_index]):
                    self._tdqResultItems.append(result_item)
        else:
            self._tdqResultItems = []

    def getCheckUUID(self):
        return self._checkUUID

    def getCheckName(self):
        return self._tdqCheckName

    def getCheckDescription(self):
        return self._tdqCheckDescription

    def getCheckParameters(self):
        return self._tdqCheckParameters

    def getGCPProjectId(self):
        return self._gcpProjectId

    def getGCPDatasetId(self):
        return self._gcpDatasetId

    def getGCPResultsTable(self):
        return self._gcpResultsTable

    def getGCPSummaryTable(self):
        return self._gcpSummaryTable

    def getCheckResultItems(self):
        return self._tdqResultItems if self._tdqResultItems is not None else []

    def getCheckItemCount(self):
        return len(self.getCheckResultItems())

    def getPassedCheckItemCount(self):
        return len(self.getPassedCheckItems())

    def getFailedCheckItemCount(self):
        return len(self.getFailedCheckItems())

    def getValidCheckItemCount(self):
        return len(self.getValidCheckItems())

    def getInvalidCheckItemCount(self):
        return len(self.getInvalidCheckItems())

    def getPassedCheckItems(self):
        return [i for i in self.getCheckResultItems() if i.isPassed()] if self.getCheckResultItems() is not None else []

    def getFailedCheckItems(self):
        return [i for i in self.getCheckResultItems() if not i.isPassed()] if self.getCheckResultItems() is not None else []

    def getValidCheckItems(self):
        return [i for i in self.getCheckResultItems() if i.isValid()] if self.getCheckResultItems() is not None else []

    def getInvalidCheckItems(self):
        return [i for i in self.getCheckResultItems() if not i.isValid()] if self.getCheckResultItems() is not None else []

    def getStartTime(self):
        return self._tdqStartTime

    def getEndTime(self):
        return self._tdqEndTime

    def getDurationSeconds(self) -> int:
        if (self.getStartTime() is not None and isinstance(self.getStartTime(), datetime)) and (self.getEndTime() is not None and isinstance(self.getEndTime(), datetime)):
            return (self.getEndTime() - self.getStartTime()).seconds
        else:
            return -1

    def getSummaryDataFrame(self):
        import pandas as pd
        from datetime import datetime
        import json

        # Prepare results dataframe
        return pd.DataFrame({
            "check_date": [datetime.now().date()],
            "check_uuid": [self.getCheckUUID()],
            "check_name": [self.getCheckName()],
            "check_description": [self.getCheckDescription()],
            "rule_count": [self.getCheckItemCount()],
            "valid_rule_count": [self.getValidCheckItemCount()],
            "invalid_rule_count": [self.getInvalidCheckItemCount()],
            "success_count": [self.getPassedCheckItemCount()],
            "failed_count": [self.getFailedCheckItemCount()],
            "execution_start_time": [self.getStartTime()],
            "execution_end_time": [self.getEndTime()],
            "is_success": [True], # TODO: Should handle failed execution
            "execution_parameters": [json.dumps(self.getCheckParameters())],
            "execution_message": [""] # TODO: Should handle failed execution exception message
        })

    def getResultsDataFrame(self):
        import pandas as pd
        # Create empty DataFrame
        df_results = pd.DataFrame()
        # Append results
        for result_item in self.getCheckResultItems():
            df_results = pd.concat([df_results, result_item.toDataFrame()], ignore_index=True)
        # Add check_date field to results DataFrame if not empty
        if (df_results is not None) and (len(df_results) > 0):
            df_results["check_date"] = datetime.now().date()
        # Return check results DataFrame
        return df_results


