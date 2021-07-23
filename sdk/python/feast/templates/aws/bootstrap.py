import inquirer

from feast.infra.utils import aws_utils


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    aws_region = inquirer.text("AWS Region (e.g. us-west-2)")
    cluster_id = inquirer.text("Redshift Cluster ID")
    database = inquirer.text("Redshift Database Name")
    user = inquirer.text("Redshift User Name")
    s3_staging_location = inquirer.text("Redshift S3 Staging Location (s3://*)")
    iam_role = inquirer.text("Redshift IAM Role for S3 (arn:aws:iam::*:role/*)")

    if inquirer.confirm(
        "Should I upload example data to Redshift (overwriting 'feast_driver_hourly_stats' table)?",
        default=True,
    ):
        client = aws_utils.get_redshift_data_client(aws_region)
        s3 = aws_utils.get_s3_resource(aws_region)

        aws_utils.execute_redshift_statement(
            client,
            cluster_id,
            database,
            user,
            f"DROP TABLE IF EXISTS feast_driver_hourly_stats",
        )

        aws_utils.upload_df_to_redshift(
            client,
            cluster_id,
            database,
            user,
            s3,
            f"{s3_staging_location}/data/feast_driver_hourly_stats.parquet",
            iam_role,
            "feast_driver_hourly_stats",
            driver_df,
        )

    repo_path = pathlib.Path(__file__).parent.absolute()
    config_file = repo_path / "feature_store.yaml"

    replace_str_in_file(config_file, "%AWS_REGION%", aws_region)
    replace_str_in_file(config_file, "%REDSHIFT_CLUSTER_ID%", cluster_id)
    replace_str_in_file(config_file, "%REDSHIFT_DATABASE%", database)
    replace_str_in_file(config_file, "%REDSHIFT_USER%", user)
    replace_str_in_file(
        config_file, "%REDSHIFT_S3_STAGING_LOCATION%", s3_staging_location
    )
    replace_str_in_file(config_file, "%REDSHIFT_IAM_ROLE%", iam_role)


def replace_str_in_file(file_path, match_str, sub_str):
    with open(file_path, "r") as f:
        contents = f.read()
    contents = contents.replace(match_str, sub_str)
    with open(file_path, "wt") as f:
        f.write(contents)


if __name__ == "__main__":
    bootstrap()
