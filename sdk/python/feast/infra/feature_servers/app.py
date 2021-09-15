import feast
from mangum import Mangum
from feast.feature_server import get_app

# TODO: replace this with an actual feature repo config deserialized from the env variables
config = feast.RepoConfig()

store = feast.FeatureStore(config=config)

app = get_app(store)

handler = Mangum(app)
