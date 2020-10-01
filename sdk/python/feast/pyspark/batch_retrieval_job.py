import json
from typing import Any, Dict, List

from pyspark import SparkFiles
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, monotonically_increasing_id, row_number


def as_of_join(
    entity: DataFrame,
    join_keys: List[str],
    feature_table: DataFrame,
    features: List[str],
    feature_prefix: str = "",
    max_age: str = None,
) -> DataFrame:
    """Perform an as of join between entity and feature table, given a maximum age tolerance.
    Join conditions:
    1. Join keys values match.
    2. Feature event timestamp is the closest match possible to the entity event timestamp,
       but must not be more recent than the entity event timestamp, and the difference must
       not be greater than max_age, unless max_age is not specified.
    3. If more than one feature table rows satisfy condition 1 and 2, feature row with the
       most recent created timestamp will be chosen.

    Args:
        entity (DataFrame):
            Entity dataframe. Must contain the column event_timestamp.
        join_keys (List[str]):
            Column names for the join keys.
        feature_table (DataFrame):
            Feature table dataframe. Must contain the columns event_timestamp and created_timestamp.
        features (List[str]):
            The feature columns which should be present in the result dataframe.
        feature_prefix (str):
            Feature column prefix for the result dataframe.
        max_age (str):
            Tolerance for the feature event timestamp recency.

    Returns:
        DataFrame: Join result.

    Example:
        >>> entity.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+

        >>> feature_table.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> df = as_of_join(entity, ["entity"], feature_table, ["feature"], feature_prefix = "prefix_")
        >>> df.show()
            +------+-------------------+--------------+
            |entity|    event_timestamp|prefix_feature|
            +------+-------------------+--------------+
            |  1001|2020-09-02 00:00:00|           200|
            +------+-------------------+--------------+

        >>> df = as_of_join(entity, ["entity"], feature_table, ["feature"], max_age = "12 hour")
        >>> df.show()
            +------+-------------------+-------+
            |entity|    event_timestamp|feature|
            +------+-------------------+-------+
            |  1001|2020-09-02 00:00:00|   null|
            +------+-------------------+-------+

    """
    entity_with_id = entity.withColumn("_row_nr", monotonically_increasing_id())

    feature_event_timestamp = f"{feature_prefix}event_timestamp"
    feature_created_timestamp = f"{feature_prefix}created_timestamp"

    projection = [
        col(col_name).alias(f"{feature_prefix}{col_name}")
        for col_name in join_keys + features + ["event_timestamp", "created_timestamp"]
    ]

    selected_feature_table = feature_table.select(projection)

    join_cond = (
        entity_with_id.event_timestamp
        >= selected_feature_table[feature_event_timestamp]
    )
    if max_age:
        join_cond = join_cond & (
            selected_feature_table[feature_event_timestamp]
            >= entity_with_id.event_timestamp - expr(f"INTERVAL {max_age}")
        )

    for key in join_keys:
        join_cond = join_cond & (
            entity_with_id[key] == selected_feature_table[f"{feature_prefix}{key}"]
        )

    conditional_join = entity_with_id.join(
        selected_feature_table, join_cond, "leftOuter"
    )
    for key in join_keys:
        conditional_join = conditional_join.drop(
            selected_feature_table[f"{feature_prefix}{key}"]
        )

    window = Window.partitionBy("_row_nr", *join_keys).orderBy(
        col(feature_event_timestamp).desc(), col(feature_created_timestamp).desc()
    )
    filter_most_recent_feature_timestamp = conditional_join.withColumn(
        "_rank", row_number().over(window)
    ).filter(col("_rank") == 1)

    return filter_most_recent_feature_timestamp.select(
        entity.columns + [f"{feature_prefix}{feature}" for feature in features]
    )


def join_entity_to_feature_tables(
    query_conf: List[Dict[str, Any]], entity: DataFrame, tables: Dict[str, DataFrame]
) -> DataFrame:
    """Perform as of join between entity and multiple feature table. Returns a DataFrame.

    Args:
        query_conf (List[Dict[str, Any]]):
            Query configuration.
        entity (DataFrame):
            Entity dataframe. Must contain the column event_timestamp.
        tables (Dict[str, DataFrame]):
            Map of feature table name to Spark DataFrame.

    Returns:
        DataFrame: Join result.

    Example:
        >>> entity.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+

        >>> feature1.show()
            +------+--------+-------------------+-------------------+
            |entity|feature1|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     200|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------

        >>> feature2.show()
            +------+--------+-------------------+-------------------+
            |entity|feature2|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------


        >>> tables = {"table1": feature1, "table2": feature2}

        >>> query_conf = [
                {
                    "table": "table1",
                    "features": ["feature1"],
                    "join": ["entity"],
                },
                {
                    "table": "table2",
                    "features": ["feature2"],
                    "join": ["entity"],
                },
            ]

        >>> joined_df = join_entity_to_feature_tables(
                query_conf,
                entity,
                tables
            )

        >>> joined_df.show()
            +------+-------------------+----------------+----------------+
            |entity|    event_timestamp|table1__feature1|table2__feature2|
            +------+-------------------+----------------+----------------+
            |  1001|2020-09-02 00:00:00|             200|             400|
            +------+-------------------+----------------+----------------+
    """
    joined = entity
    for query in query_conf:
        joined = as_of_join(
            joined,
            query["join"],
            tables[query["table"]],
            query["features"],
            feature_prefix=f"{query['table']}__",
            max_age=query.get("max_age"),
        )
    return joined


def batch_retrieval(spark: SparkSession, conf: Dict) -> DataFrame:
    """Retrieve batch features based on given configuration.

    Args:
        spark (SparkSession):
            Spark session.
        conf (Dict):
            Configuration for the retrieval job, in json format. Sample configuration as follows:

    Returns:
        DataFrame: Join result.

    Example:
        sample_conf = {
            "entity": {
                "format": "csv",
                "path": "file:///some_dir/customer_driver_pairs.csv",
                "options": {"inferSchema": "true", "header": "true"},
                "col_mapping": {
                    "id": "driver_id"
                }
            },
            "tables": [
                {
                    "format": "parquet",
                    "path": "gs://some_bucket/bookings.parquet",
                    "name": "bookings",
                    "col_mapping": {
                        "id": "driver_id"
                    }
                },
                {
                    "format": "avro",
                    "path": ""s3://some_bucket/transactions.parquet"",
                    "name": "transactions",
                },
            ],
            "queries": [
                {
                    "table": "transactions",
                    "features": ["daily_transactions"],
                    "join": ["customer_id"],
                    "max_age": "2 day",
                },
                {
                    "table": "bookings",
                    "features": ["completed_bookings"],
                    "join": ["driver_id"],
                },
            ],
            "output":
                "format": "parquet"
                "path": "gs://some_bucket/output.parquet"
        }

        The values for the `format` and `path` should be recognizable by the Spark cluster where the job
        is going to run on. For example, if you specify `bigquery` as input format, then you should ensure
        that the Spark Big Query connector is installed on the cluster. Like wise, s3a connector is required
        for Amazon S3 path.

        `options` is optional. If present, the options will be used when reading / writing the input / output.

        If necessary, `col_mapping` can be provided to map the columns of the dataframes before performing
        the join operation. `col_mapping` is a dictionary where the key is the source column and the value
        is the mapped column.

    """

    def map_column(df: DataFrame, col_mapping: Dict[str, str]):
        projection = [
            col(col_name).alias(col_mapping.get(col_name, col_name))
            for col_name in df.columns
        ]
        return df.select(projection)

    entity = conf["entity"]
    entity_df = (
        spark.read.format(entity["format"])
        .options(**entity.get("options", {}))
        .load(entity["path"])
    )
    entity_col_mapping = conf["entity"].get("col_mapping", {})
    mapped_entity_df = map_column(entity_df, entity_col_mapping)

    tables = {
        table_spec["name"]: map_column(
            spark.read.format(table_spec["format"])
            .options(**table_spec.get("options", {}))
            .load(table_spec["path"]),
            table_spec.get("col_mapping", {}),
        )
        for table_spec in conf["tables"]
    }

    return join_entity_to_feature_tables(conf["queries"], mapped_entity_df, tables)


def start_job(spark: SparkSession, conf: Dict):
    result = batch_retrieval(spark, conf)
    output = conf["output"]
    result.write.format(output["format"]).options(**output.get("options", {})).mode(
        "overwrite"
    ).save(output["path"])


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Batch Retrieval").getOrCreate()
    spark.sparkContext.addFile("config.json")
    config_file_path = SparkFiles.get("config.json")
    with open(config_file_path, "r") as config_file:
        conf = json.load(config_file)
        start_job(spark, conf)
    spark.stop()
