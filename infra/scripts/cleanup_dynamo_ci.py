import boto3


def main() -> None:
    db = boto3.resource("dynamodb")

    num_deleted = 0
    for table in db.tables.all():
        if "integration_test" in table.name:
            table.delete()
            num_deleted += 1
    print(f"Deleted {num_deleted} CI DynamoDB tables")


if __name__ == "__main__":
    main()
