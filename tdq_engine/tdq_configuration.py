class TDQConfiguration:

    def __init__(self, tdq_check_name: str = "", tdq_check_description: str = "", tdq_check_parameters: dict = {}):
        self._tdq_check_name = tdq_check_name
        self._tdq_check_description = tdq_check_description
        self._tdq_check_parameters = tdq_check_parameters

    def setTDQCheckName(self, tdq_check_name: str):
        self._tdq_check_name = tdq_check_name

    def setTDQCheckDescription(self, tdq_check_description: str):
        self._tdq_check_description = tdq_check_description

    def addTDQCheckParameter(self, key: str, value: any = None):
        self._tdq_check_parameters[key] = value

    def getTDQCheckName(self):
        return self._tdq_check_name

    def getTDQCheckDescription(self):
        return self._tdq_check_description

    def getTDQParameter(self, key: str):
        return self._tdq_check_parameters.get(key, None)

    def getTDQParameters(self):
        return self._tdq_check_parameters
