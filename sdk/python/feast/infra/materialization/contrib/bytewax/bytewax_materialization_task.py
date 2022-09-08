from feast.infra.materialization.batch_materialization_engine import MaterializationTask


class BytewaxMaterializationTask(MaterializationTask):
    def __init__(self, project, feature_view, start_date, end_date, tqdm):
        self.project = project
        self.feature_view = feature_view
        self.start_date = start_date
        self.end_date = end_date
        self.tqdm = tqdm
