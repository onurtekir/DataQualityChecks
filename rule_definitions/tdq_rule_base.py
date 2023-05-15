from enum import Enum
import uuid
from abc import abstractmethod


class RULE_TYPE(Enum):
    ROW_BASED = "ROW_BASED"
    TABLE_BASED = "TABLE_BASED"


class TDQRuleBase:
    import json

    def __init__(self, rule_type: RULE_TYPE = None,  rule_check_type: str = None, column_name: str = None, threshold: float = 0.0):
        self._initialize()
        self._ruleCheckUUID = uuid.uuid4()
        self._ruleType = rule_type
        self.setRuleCheckType(check_type=rule_check_type)
        self.setColumnName(column_name=column_name)
        self.setThreshold(threshold=threshold)

    def _initialize(self):
        self._ruleType: RULE_TYPE = None
        self._checkUUID: uuid.UUID = None
        self._ruleCheckUUID: uuid.UUID = None
        self._checkType: str = None
        self._parameters: dict = {}
        self._threshold: float = None
        self._columnName: str = None

    # region Abstract Method

    @abstractmethod
    def _prepare_rule_sql(self):
        pass

    # endregion

    # region Getters/Setters

    def setCheckUUID(self, base_uuid: uuid.UUID):
        self._checkUUID = base_uuid

    def setRuleCheckType(self, check_type: str):
        self._checkType = check_type

    def setColumnName(self, column_name):
        self._columnName = column_name

    def setThreshold(self, threshold: float):
        self._threshold = threshold

    def setParameter(self, key: str, value: any):
        self._parameters[key] = value

    def getCheckUUID(self):
        if self._checkUUID is None:
            return uuid.UUID(int=0)
        else:
            return self._checkUUID

    def getRuleCheckUUID(self):
        if self._ruleCheckUUID is None:
            self._ruleCheckUUID = uuid.uuid4()
        return self._ruleCheckUUID

    def getRuleCheckType(self):
        return self._checkType

    def getRuleType(self):
        return self._ruleType

    def getParameters(self):
        return self._parameters

    def getParameter(self, key: str, default: any = None):
        return self._parameters.get(key, default)

    def getRuleSQL(self, check_uuid: uuid = None):
        self.setCheckUUID(base_uuid=check_uuid)
        return self._prepare_rule_sql()

    def getColumnName(self):
        return self._columnName

    def getThreshold(self):
        return self._threshold

    def getBaseCTE(self):
        base_cte = f"cte_query_base_{str(self.getCheckUUID()).replace('-', '_')}"
        return base_cte

    def isValid(self):
        # Check if mandatory parameters set
        return (self.getCheckUUID() is not None) & \
               (self.getRuleCheckUUID() is not None) & \
               (self.getRuleCheckType() is not None) & \
               (self.getColumnName() is not None) & \
               (self.getThreshold() is not None) & \
               (self.getRuleType() is not None)

    def printConfiguration(self):
        print(f"Rule Valid: {self.isValid()}")
        print(f"Rule Check UUID: {str(self.getRuleCheckUUID())}")
        print(f"Type: {str(self.getRuleType().value)}")
        print(f"Rule Check Type: {self.getRuleCheckType()}")
        print(f"Column Name: {self.getColumnName()}")
        if (self._parameters is not None) and (isinstance(self._parameters, dict)):
            for param in self._parameters:
                print(f"\t{param}: {self._parameters[param]}")
        print(f"Threshold: {self.getThreshold()}")
        print(f"QUERY")
        print(self.getRuleSQL())

    # endregion
