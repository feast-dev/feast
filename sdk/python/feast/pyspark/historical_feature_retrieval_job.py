import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, NamedTuple, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, monotonically_increasing_id, row_number


class EntitySource(NamedTuple):
    """
    Source for the entity dataframe. The values for the `format` and `path` should be
    recognizable by the Spark cluster where the job is going to run on. For example, s3a
    connector is required for Amazon S3 path. Spark avro package should be on the class
    path of the spark cluster if the format is `avro`.

    Attributes:
        format (str): File format.
        path (str): uri to the file source.
        timestamp_column (str): Column representing the event timestamp.
        mapping (Dict[str, str]): Optional. If present, the source column will be renamed
            based on the mapping.
        options (Dict[str, str]): Spark read options.
    """

    format: str
    path: str
    timestamp_column: str
    mapping: Dict[str, str] = {}
    options: Dict[str, str] = {}


class FeatureTableSource(NamedTuple):
    """
    Source for the Feature Table dataframe. The values for the `format` and `path` should be
    recognizable by the Spark cluster where the job is going to run on. For example, s3a
    connector is required for Amazon S3 path. Spark avro package should be on the class
    path of the spark cluster if the format is `avro`.

    Attributes:
        format (str): File format.
        path (str): uri to the file source.
        timestamp_column (str): Column representing the event timestamp.
        created_timestamp_column (str): Column representing the creation timestamp. Not required for entity.
        mapping (Dict[str, str]): Optional. If present, the source column will be renamed
            based on the mapping.
        options (Dict[str, str]): Spark read options.
    """

    format: str
    path: str
    timestamp_column: str
    created_timestamp_column: str
    mapping: Dict[str, str] = {}
    options: Dict[str, str] = {}


class Field(NamedTuple):
    """
    Defines name and type for Feast entities and features.

    Attributes:
        name (str): Field name.
        type (str): Feast type name.
    """

    name: str
    type: str

    @property
    def spark_type(self):
        """
        Returns Spark data type that corresponds to the field's Feast type
        """
        feast_to_spark_type_mapping = {
            "bytes": "binary",
            "string": "string",
            "int32": "int",
            "int64": "bigint",
            "double": "double",
            "float": "float",
            "bool": "boolean",
            "bytes_list": "array<binary>",
            "string_list": "array<string>",
            "int32_list": "array<int>",
            "int64_list": "array<bigint>",
            "double_list": "array<double>",
            "float_list": "array<float>",
            "bool_list": "array<boolean>",
        }
        return feast_to_spark_type_mapping[self.type.lower()]


class FeatureTable(NamedTuple):
    """
    Feature table specification.

    Attributes:
        name (str): Table name.
        entities (List[Field]): Primary keys for the features.
        features (List[Field]): Feature list.
        max_age (int): In seconds. determines the lower bound of the timestamp of the retrieved feature.
            If not specified, this would be unbounded
        project (str): Feast project name.
    """

    name: str
    entities: List[Field]
    features: List[Field]
    max_age: Optional[int] = None
    project: Optional[str] = None

    @property
    def entity_names(self):
        """
        Returns columns names for the entities.
        """
        return [field.name for field in self.entities]

    @property
    def feature_names(self):
        """
        Returns columns names for the features.
        """
        return [field.name for field in self.features]


class FileDestination(NamedTuple):
    """
    Output file for the spark job.

    Attributes:
        format (str): Output format.
        path (str): Output uri.
    """

    format: str
    path: str


class EntityDataframe(NamedTuple):
    """
    Entity dataframe with specification.

    Attributes:
        df (DataFrame): Dataframe for the table.
        timestamp_column (str): Column representing the event timestamp.
    """

    df: DataFrame
    timestamp_column: str

    @classmethod
    def from_source(cls, spark: SparkSession, source: EntitySource):
        """
        Construct an EntityDataframe instance based on the Source.

        Args:
            spark (SparkSession): Spark session.
            source (EntitySource): Entity table source.

        Returns:
            EntityDataframe: Instance of EntityDataframe.
        """

        df = (
            spark.read.format(source.format).options(**source.options).load(source.path)
        )

        mapped_entity_df = _map_column(df, source.mapping)

        return EntityDataframe(
            df=mapped_entity_df, timestamp_column=source.timestamp_column
        )


class FeatureTableDataframe(NamedTuple):
    """
    Feature table dataframe with specification.

    Attributes:
        name (str): Table name.
        df (DataFrame): Dataframe for the table.
        timestamp_column (str): Column representing the event timestamp.
        created_timestamp_column (str): Column representing the creation timestamp.
        max_age (int): In seconds. determines the lower bound of the timestamp of the retrieved feature.
            If not specified, this would be unbounded
        entities (List[Field]): Primary keys for the features.
        features (List[Field]): Feature list.
    """

    name: str
    df: DataFrame
    timestamp_column: str
    created_timestamp_column: str
    entities: List[Field]
    features: List[Field]
    max_age: Optional[int] = None

    @property
    def entity_names(self):
        """
        Returns columns names for the entities.
        """
        return [field.name for field in self.entities]

    @property
    def feature_names(self):
        """
        Returns columns names for the features.
        """
        return [field.name for field in self.features]

    @classmethod
    def from_feature_table_and_source(
        cls,
        spark: SparkSession,
        feature_table: FeatureTable,
        source: FeatureTableSource,
        entity_min_timestamp: datetime,
        entity_max_timestamp: datetime,
    ):
        """
        Construct a FeatureTableDataframe instance based on FeatureTable and the
        corresponding Source. The entity minimum and maximum timestamp, along with
        the max age of the feature table, will be used to compute the lower and upper
        bound of the feature table event timestamp.

        Args:
            spark (SparkSession): Spark session.
            feature_table (FeatureTable): Feature table specification.
            source (FeatureTableSource): Feature table source.
            entity_min_timestamp (datetime): Minimum datetime for the entity dataframe.
            entity_max_timestamp (datetime): Maximum datetime for the entity dataframe.

        Returns:
            FeatureTableDataframe: Instance of FeatureTableDataframe.

        Raises:
            SchemaError: If the feature table has missing columns or wrong column types.
        """
        source_df = (
            spark.read.format(source.format).options(**source.options).load(source.path)
        )

        mapped_source_df = _map_column(source_df, source.mapping)

        column_selection = (
            feature_table.feature_names
            + feature_table.entity_names
            + [source.timestamp_column, source.created_timestamp_column]
        )

        missing_columns = set(column_selection) - set(mapped_source_df.columns)
        if len(missing_columns) > 0:
            raise SchemaError(
                f"{', '.join(missing_columns)} are missing for feature table {feature_table.name}."
            )

        feature_table_dtypes = dict(mapped_source_df.dtypes)
        for field in feature_table.entities + feature_table.features:
            column_type = feature_table_dtypes.get(field.name)
            if column_type != field.spark_type:
                raise SchemaError(
                    f"{field.name} should be of {field.spark_type} type, but is {column_type} instead"
                )

        for timestamp_column in [
            source.timestamp_column,
            source.created_timestamp_column,
        ]:
            column_type = feature_table_dtypes.get(timestamp_column)
            if column_type != "timestamp":
                raise SchemaError(
                    f"{timestamp_column} should be of timestamp type, but is {column_type} instead"
                )

        subset_source_df = mapped_source_df.select(
            feature_table.feature_names
            + feature_table.entity_names
            + [source.timestamp_column, source.created_timestamp_column]
        )

        feature_table_timestamp_filter = (
            col(source.timestamp_column).between(
                entity_min_timestamp - timedelta(seconds=feature_table.max_age),
                entity_max_timestamp,
            )
            if feature_table.max_age
            else col(source.timestamp_column) <= entity_max_timestamp
        )

        time_range_filtered_df = subset_source_df.filter(feature_table_timestamp_filter)

        return FeatureTableDataframe(
            name=feature_table.name,
            df=time_range_filtered_df,
            timestamp_column=source.timestamp_column,
            created_timestamp_column=source.created_timestamp_column,
            max_age=feature_table.max_age,
            entities=feature_table.entities,
            features=feature_table.features,
        )


def _map_column(df: DataFrame, col_mapping: Dict[str, str]):
    projection = [
        col(col_name).alias(col_mapping.get(col_name, col_name))
        for col_name in df.columns
    ]
    return df.select(projection)


def as_of_join(
    entity: EntityDataframe, feature_table: FeatureTableDataframe,
) -> DataFrame:
    """Perform an as of join between entity and feature table, given a maximum age tolerance.
    Join conditions:
    1. Entity primary key(s) value matches.
    2. Feature event timestamp is the closest match possible to the entity event timestamp,
       but must not be more recent than the entity event timestamp, and the difference must
       not be greater than max_age, unless max_age is not specified.
    3. If more than one feature table rows satisfy condition 1 and 2, feature row with the
       most recent created timestamp will be chosen.
    4. If none of the above conditions are satisfied, the feature rows will have null values.

    Args:
        entity (EntityDataFrame): Entity dataframe and specification.
        feature_table (FeatureTableDataframe): Feature table dataframe and specification.

    Returns:
        DataFrame: Join result.

    Example:
        >>> entity.df.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+

        >>> feature_table_1.df.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> feature_table_1.max_age
            None
        >>> feature_table_1.name
            'table1'
        >>> df = as_of_join(entity, feature_table_1)
        >>> df.show()
            +------+-------------------+---------------+
            |entity|    event_timestamp|table1__feature|
            +------+-------------------+---------------+
            |  1001|2020-09-02 00:00:00|            200|
            +------+-------------------+---------------+

        >>> feature_table_2.df.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> feature_table_2.max_age
            43200
        >>> feature_table_2.name
            'table2'
        >>> df = as_of_join(entity, feature_table_2)
        >>> df.show()
            +------+-------------------+---------------+
            |entity|    event_timestamp|table2__feature|
            +------+-------------------+---------------+
            |  1001|2020-09-02 00:00:00|           null|
            +------+-------------------+---------------+

    """
    entity_with_id = entity.df.withColumn("_row_nr", monotonically_increasing_id())

    feature_event_timestamp = f"{feature_table.name}__{feature_table.timestamp_column}"
    feature_created_timestamp = (
        f"{feature_table.name}__{feature_table.created_timestamp_column}"
    )

    projection = [
        col(col_name).alias(f"{feature_table.name}__{col_name}")
        for col_name in feature_table.df.columns
    ]

    aliased_feature_table = feature_table.df.select(projection)

    join_cond = (
        entity_with_id[entity.timestamp_column]
        >= aliased_feature_table[feature_event_timestamp]
    )
    if feature_table.max_age:
        join_cond = join_cond & (
            aliased_feature_table[feature_event_timestamp]
            >= entity_with_id[entity.timestamp_column]
            - expr(f"INTERVAL {feature_table.max_age} seconds")
        )

    for key in feature_table.entity_names:
        join_cond = join_cond & (
            entity_with_id[key] == aliased_feature_table[f"{feature_table.name}__{key}"]
        )

    conditional_join = entity_with_id.join(
        aliased_feature_table, join_cond, "leftOuter"
    )
    for key in feature_table.entity_names:
        conditional_join = conditional_join.drop(
            aliased_feature_table[f"{feature_table.name}__{key}"]
        )

    window = Window.partitionBy("_row_nr", *feature_table.entity_names).orderBy(
        col(feature_event_timestamp).desc(), col(feature_created_timestamp).desc()
    )
    filter_most_recent_feature_timestamp = conditional_join.withColumn(
        "_rank", row_number().over(window)
    ).filter(col("_rank") == 1)

    return filter_most_recent_feature_timestamp.select(
        entity.df.columns
        + [
            f"{feature_table.name}__{feature}"
            for feature in feature_table.feature_names
        ]
    )


def join_entity_to_feature_tables(
    entity: EntityDataframe, feature_tables: List[FeatureTableDataframe]
) -> DataFrame:
    """Perform as of join between entity and multiple feature table. Returns a DataFrame.

    Args:
        entity (EntityDataFrame):
            Entity dataframe and specification.
        feature_tables (List[FeatureTableDataframe]):
            List of feature tables dataframes and their corresponding specification.

    Returns:
        DataFrame: Join result.

    Example:
        >>> entity_df.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+

        >>> entity = EntityDataframe(entity_df, "event_timestamp")

        >>> table1_df.show()
            +------+--------+-------------------+-------------------+
            |entity|feature1|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     200|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------

        >>> table1 = FeatureTableDataframe(
                name="table1",
                df=table1_df,
                features=[Field("feature1", "int32")],
                entities=[Field("entity", "int32")],
                timestamp_column="event_timestamp",
                created_timestamp_column="created_timestamp",
            )

        >>> table2_df.show()
            +------+--------+-------------------+-------------------+
            |entity|feature2|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------

        >>> table2 = FeatureTableDataframe(
                name="table2",
                df=table2_df,
                features=[Field("feature2", "int32")],
                entities=[Field("entity", "int32")],
                timestamp_column="event_timestamp",
                created_timestamp_column="created_timestamp",
            )


        >>> tables = [table1, table2]

        >>> joined_df = join_entity_to_feature_tables(
                entity,
                tables,
            )

        >>> joined_df.show()
            +------+-------------------+----------------+----------------+
            |entity|    event_timestamp|table1__feature1|table2__feature2|
            +------+-------------------+----------------+----------------+
            |  1001|2020-09-02 00:00:00|             200|             400|
            +------+-------------------+----------------+----------------+
    """
    joined = entity

    for feature_table in feature_tables:
        joined_df = as_of_join(joined, feature_table)
        joined = EntityDataframe(joined_df, entity.timestamp_column)
    return joined.df


class SchemaError(Exception):
    """
    One or more columns in entity or feature table dataframe are either missing
    or have the wrong data types
    """

    pass


def retrieve_historical_features(
    spark: SparkSession,
    entity_source: EntitySource,
    feature_tables_sources: List[FeatureTableSource],
    feature_tables: List[FeatureTable],
) -> DataFrame:
    """Retrieve historical features based on given configurations.

    Args:
        spark (SparkSession): Spark session.
        entity_source (EntitySource): Entity data source.
        feature_tables_sources (FeatureTableSource): List of feature tables data sources.
        feature_tables (List[FeatureTable]): List of feature table specification.
            The order of the feature table must correspond to that of feature_tables_sources.

    Returns:
        DataFrame: Join result.

    Example:
        >>> entity_source = EntitySource(
                format="csv",
                path="file:///some_dir/customer_driver_pairs.csv"),
                options={"inferSchema": "true", "header": "true"},
                mapping={"id": "driver_id"}
            )

        >>> feature_tables_sources = [
                Source(
                    format="parquet",
                    path="gs://some_bucket/bookings.parquet"),
                    mapping={"id": "driver_id"}
                ),
                Source(
                    format="avro",
                    path="s3://some_bucket/transactions.avro"),
                )
            ]

        >>> feature_tables = [
                FeatureTable(
                    name="bookings",
                    entities=[Field("driver_id", "int32")],
                    features=[Field("completed_bookings", "int32")],
                ),
                FeatureTable(
                    name="transactions",
                    entities=[Field("customer_id", "int32")],
                    features=[Field("total_transactions", "double")],
                    max_age=172800
                ),
            ]
    """

    entity_df = (
        spark.read.format(entity_source.format)
        .options(**entity_source.options)
        .load(entity_source.path)
    )

    mapped_entity_df = _map_column(entity_df, entity_source.mapping)

    if entity_source.timestamp_column not in mapped_entity_df.columns:
        raise SchemaError(
            f"{entity_source.timestamp_column} is missing for the entity dataframe."
        )

    max_timestamp = mapped_entity_df.agg(
        {entity_source.timestamp_column: "max"}
    ).collect()[0][0]
    min_timestamp = mapped_entity_df.agg(
        {entity_source.timestamp_column: "min"}
    ).collect()[0][0]

    tables = [
        FeatureTableDataframe.from_feature_table_and_source(
            spark, feature_table, source, min_timestamp, max_timestamp
        )
        for feature_table, source in zip(feature_tables, feature_tables_sources)
    ]

    entity = EntityDataframe(mapped_entity_df, entity_source.timestamp_column)

    expected_entities = []
    for table in tables:
        expected_entities.extend(table.entities)

    entity_dtypes = dict(entity.df.dtypes)
    for expected_entity in expected_entities:
        if entity_dtypes.get(expected_entity.name) != expected_entity.spark_type:
            raise SchemaError(
                f"{expected_entity.name} ({expected_entity.spark_type}) is not present in the entity dataframe."
            )

    return join_entity_to_feature_tables(entity, tables)


def start_job(
    spark: SparkSession,
    entity_source: EntitySource,
    feature_tables_sources: List[FeatureTableSource],
    feature_tables: List[FeatureTable],
    destination: FileDestination,
):
    result = retrieve_historical_features(
        spark, entity_source, feature_tables_sources, feature_tables
    )
    result.write.format(destination.format).mode("overwrite").save(destination.path)


def _get_args():
    parser = argparse.ArgumentParser(description="Retrieval job arguments")
    parser.add_argument(
        "--feature-tables", type=str, help="Feature table list in json string"
    )
    parser.add_argument(
        "--feature-tables-sources",
        type=str,
        help="Feature table source list in json string",
    )
    parser.add_argument(
        "--entity-source", type=str, help="Entity source in json string"
    )
    parser.add_argument(
        "--destination", type=str, help="Retrieval result destination in json string"
    )
    return parser.parse_args()


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    args = _get_args()
    feature_tables = [FeatureTable(**dct) for dct in json.loads(args.feature_tables)]
    feature_tables_sources = [
        FeatureTableSource(**dct) for dct in json.loads(args.feature_tables_source)
    ]
    entity_source = EntitySource(**json.loads(args.entity_source))
    destination = FileDestination(**json.loads(args.destination))
    start_job(spark, entity_source, feature_tables_sources, feature_tables, destination)
    spark.stop()
