from feast.feature_set import FeatureSet


class ResourceFactory:
    @staticmethod
    def get_resource(kind):
        if kind == "feature_set":
            return FeatureSet
        else:
            raise ValueError(kind)
