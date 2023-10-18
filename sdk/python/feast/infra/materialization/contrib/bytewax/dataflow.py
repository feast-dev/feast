import yaml

from feast import FeatureStore, RepoConfig
from feast.infra.materialization.contrib.bytewax.bytewax_materialization_dataflow import (
    BytewaxMaterializationDataflow,
)

if __name__ == "__main__":
    with open("/var/feast/feature_store.yaml") as f:
        feast_config = yaml.safe_load(f)

        with open("/var/feast/bytewax_materialization_config.yaml") as b:
            bytewax_config = yaml.safe_load(b)

            config = RepoConfig(**feast_config)
            store = FeatureStore(config=config)

            job = BytewaxMaterializationDataflow(
                config,
                store.get_feature_view(bytewax_config["feature_view"]),
                bytewax_config["paths"],
            )
