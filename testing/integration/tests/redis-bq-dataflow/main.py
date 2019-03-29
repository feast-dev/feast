import test_feast
from feast.sdk.client import Client

if __name__ == "__main__":
    feast_client = Client(verbose=True)
    test_feast.TestFeastIntegration().test_end_to_end(client=feast_client)
