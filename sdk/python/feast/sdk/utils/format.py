from feast.types.Granularity_pb2 import Granularity


def make_feature_id(entity, granularity, feature_name):
    """
    Create formatted feature ID
    :param entity: entity name
    :param granularity: granularity of feature
    :param feature_name: feature name
    :return: formatted feature ID as {entity}.{granularity}.{feature_name}
    """
    return ".".join([entity, Granularity.Enum.Name(granularity.value),
                     feature_name]).lower()