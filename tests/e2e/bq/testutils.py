from deepdiff import DeepDiff
from google.protobuf.json_format import MessageToDict


def clear_unsupported_fields(datasets):
    dataset = datasets.datasets[0]
    for feature in dataset.features:
        if feature.HasField("num_stats"):
            feature.num_stats.common_stats.ClearField("num_values_histogram")
            # Since difference in how BQ and TFDV compute histogram values make them
            # approximate but uncomparable
            feature.num_stats.ClearField("histograms")
        elif feature.HasField("string_stats"):
            feature.string_stats.common_stats.ClearField("num_values_histogram")
            for bucket in feature.string_stats.rank_histogram.buckets:
                bucket.ClearField("low_rank")
                bucket.ClearField("high_rank")
        elif feature.HasField("struct_stats"):
            feature.string_stats.struct_stats.ClearField("num_values_histogram")
        elif feature.HasField("bytes_stats"):
            feature.string_stats.bytes_stats.ClearField("num_values_histogram")


def clear_unsupported_agg_fields(datasets):
    dataset = datasets.datasets[0]
    for feature in dataset.features:
        if feature.HasField("num_stats"):
            feature.num_stats.common_stats.ClearField("num_values_histogram")
            feature.num_stats.ClearField("histograms")
            feature.num_stats.ClearField("median")
        elif feature.HasField("string_stats"):
            feature.string_stats.common_stats.ClearField("num_values_histogram")
            feature.string_stats.ClearField("rank_histogram")
            feature.string_stats.ClearField("top_values")
            feature.string_stats.ClearField("unique")
        elif feature.HasField("struct_stats"):
            feature.struct_stats.ClearField("num_values_histogram")
        elif feature.HasField("bytes_stats"):
            feature.bytes_stats.ClearField("num_values_histogram")
            feature.bytes_stats.ClearField("unique")


def assert_stats_equal(left, right):
    left_stats = MessageToDict(left)["datasets"][0]
    right_stats = MessageToDict(right)["datasets"][0]
    assert (
        left_stats["numExamples"] == right_stats["numExamples"]
    ), f"Number of examples do not match. Expected {left_stats['numExamples']}, got {right_stats['numExamples']}"

    left_features = sorted(left_stats["features"], key=lambda k: k["path"]["step"][0])
    right_features = sorted(right_stats["features"], key=lambda k: k["path"]["step"][0])
    diff = DeepDiff(left_features, right_features, significant_digits=3)
    assert (
        len(diff) == 0
    ), f"Feature statistics do not match: \nwanted: {left_features}\n got: {right_features}"
