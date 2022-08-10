import pytest


@pytest.mark.benchmark
@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_online_retrieval(feature_store_for_online_retrieval, benchmark):
    """
    Benchmarks a basic online retrieval flow.
    """
    fs, feature_refs, entity_rows = feature_store_for_online_retrieval
    benchmark(
        fs.get_online_features,
        features=feature_refs,
        entity_rows=entity_rows,
    )
