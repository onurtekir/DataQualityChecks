class TDQGoogleCloudConfiguration:

    def __init__(self, project_id: str = "", dataset_id: str = "", tdq_summary_table: str = "", tdq_results_table: str = ""):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._tdq_summary_table = tdq_summary_table
        self._tdq_results_table = tdq_results_table

    def setProjectId(self, project_id: str = None):
        self._project_id = project_id

    def setDatasetId(self, dataset_id: str = None):
        self._dataset_id = dataset_id

    def setTDQSummaryTable(self, tdq_summary_table: str = None):
        self._tdq_summary_table = tdq_summary_table

    def setTDQResultsTable(self, tdq_results_table: str = None):
        self._tdq_results_table = tdq_results_table

    def getProjectId(self):
        return self._project_id

    def getDatasetId(self):
        return self._dataset_id

    def getTDQSummaryTable(self):
        return self._tdq_summary_table

    def getTDQResultsTable(self):
        return self._tdq_results_table
