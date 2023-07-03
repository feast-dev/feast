from time import sleep
import boto3
from tqdm import tqdm
from google.cloud import bigtable
from google.cloud.bigtable import enums


def cleanup_dynamo_ci():
    db = boto3.resource("dynamodb")

    num_to_delete = 0
    all_tables = db.tables.all()
    for table in all_tables:
        if "integration_test" in table.name:
            num_to_delete += 1
    with tqdm(total=num_to_delete) as progress:
        for table in all_tables:
            if "integration_test" in table.name:
                table.delete()
                progress.update()
    print(f"Deleted {num_to_delete} CI DynamoDB tables")


def cleanup_bigtable_ci():
    client = bigtable.Client(project="kf-feast", admin=True)
    instance = client.instance("feast-integration-tests")
    if instance.exists():
        print(f"Deleted Bigtable CI instance")
        instance.delete()

    location_id = "us-central1-f"
    serve_nodes = 1
    storage_type = enums.StorageType.SSD
    cluster = instance.cluster(
        "feast-integration-tests-c1",
        location_id=location_id,
        serve_nodes=serve_nodes,
        default_storage_type=storage_type,
    )
    instance.create(clusters=[cluster])
    print(f"Created new Bigtable CI tables")


def main() -> None:
    cleanup_dynamo_ci()
    cleanup_bigtable_ci()


if __name__ == "__main__":
    main()
