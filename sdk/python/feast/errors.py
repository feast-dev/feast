class FeastObjectNotFoundException(Exception):
    pass


class EntityNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Entity {name} does not exist in project {project}")
        else:
            super().__init__(f"Entity {name} does not exist")


class FeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Feature view {name} does not exist in project {project}")
        else:
            super().__init__(f"Feature view {name} does not exist")


class FeatureTableNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"Feature table {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature table {name} does not exist")


class FeastProviderLoginError(Exception):
    """Error class that indicates a user has not authenticated with their provider."""
    def __init__(self, project, name):
        super().__init__(f"Feature table {name} does not exist in project {project}")


class ProviderNameParsingException(Exception):
    def __init__(self, provider_name):
        super().__init__(
            f"Could not parse provider name '{provider_name}' into module and class names"
        )


class ProviderModuleImportError(Exception):
    def __init__(self, module_name):
        super().__init__(f"Could not import provider module '{module_name}'")


class ProviderClassImportError(Exception):
    def __init__(self, module_name, class_name):
        super().__init__(
            f"Could not import provider '{class_name}' from module '{module_name}'"
        )
