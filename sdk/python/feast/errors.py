class FeastObjectNotFoundException(Exception):
    pass


class EntityNotFoundException(FeastObjectNotFoundException):
    def __init__(self, project, name):
        super().__init__(f"Entity {name} does not exist in project {project}")


class FeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, project, name):
        super().__init__(f"Feature view {name} does not exist in project {project}")


class FeatureTableNotFoundException(FeastObjectNotFoundException):
    def __init__(self, project, name):
        super().__init__(f"Feature table {name} does not exist in project {project}")
