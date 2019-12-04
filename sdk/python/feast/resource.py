from feast.feature_set import FeatureSet

# TODO: This factory adds no value. It should be removed asap.
class ResourceFactory:
    @staticmethod
    def get_resource(kind):
        if kind == "feature_set":
            return FeatureSet
        else:
            raise ValueError(kind)
