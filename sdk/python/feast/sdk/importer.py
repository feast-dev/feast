# Copyright 2018 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
import ntpath
import time
import datetime
from feast.specs.ImportSpec_pb2 import ImportSpec, Schema
from feast.sdk.utils.gs_utils import gcs_to_df, is_gs_path, df_to_gcs
from feast.sdk.utils.print_utils import spec_to_yaml
from feast.sdk.utils.types import dtype_to_value_type
from feast.sdk.utils.bq_util import head
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.entity import Entity

from google.protobuf.timestamp_pb2 import Timestamp
from google.cloud import bigquery


class Importer:
    def __init__(self, specs, df, properties):
        self._properties = properties
        self._specs = specs
        self.df = df

    @property
    def source(self):
        """str: source of the data"""
        return self._properties.get("source")

    @property
    def size(self):
        """str: number of rows in the data"""
        return self._properties.get("size")

    @property
    def require_staging(self):
        """bool: whether the data needs to be staged"""
        return self._properties.get("require_staging")

    @property
    def remote_path(self):
        """str: remote path of the file"""
        return self._properties.get("remote_path")

    @property
    def spec(self):
        """feast.specs.ImportSpec_pb2.ImportSpec: 
            import spec for this dataset"""
        return self._specs.get("import")

    @property
    def features(self):
        """list[feast.specs.FeatureSpec_pb2.FeatureSpec]: 
            list of features associated with this dataset"""
        return self._specs.get("features")

    @property
    def entity(self):
        """feast.specs.EntitySpec_pb2.EntitySpec: 
            entity associated with this dataset"""
        return self._specs.get("entity")

    @classmethod
    def from_csv(cls,
                 path,
                 entity,
                 owner,
                 staging_location=None,
                 id_column=None,
                 feature_columns=None,
                 timestamp_column=None,
                 timestamp_value=None,
                 serving_store=None,
                 warehouse_store=None,
                 job_options={}):
        """Creates an importer from a given csv dataset. 
        This file can be either local or remote (in gcs). If it's a local file 
        then staging_location must be determined.
        
        Args:
            path (str): path to csv file
            entity (str): entity id
            owner (str): owner
            staging_location (str, optional): Defaults to None. Staging location 
                for ingesting a local csv file.
            id_column (str, optional): Defaults to None. Id column in the csv. 
                If not set, will default to the `entity` argument.
            feature_columns ([str], optional): Defaults to None. Feature columns
                to ingest. If not set, the importer will by default ingest all 
                available columns.
            timestamp_column (str, optional): Defaults to None. Timestamp 
                column in the csv. If not set, defaults to timestamp value.
            timestamp_value (datetime, optional): Defaults to current datetime. 
                Timestamp value to assign to all features in the dataset.
            serving_store (feast.sdk.resources.feature.DataStore): Defaults to None.
                Serving store to write the features in this instance to.
            warehouse_store (feast.sdk.resources.feature.DataStore): Defaults to None.
                Warehouse store to write the features in this instance to.
            job_options (dict): Defaults to empty dict. Additional job options.
        
        Returns:
            Importer: the importer for the dataset provided.
        """
        src_type = "file.csv"
        source_options = {}
        source_options["path"], require_staging = \
            _get_remote_location(path, staging_location)
        if is_gs_path(path):
            df = gcs_to_df(path)
        else:
            df = pd.read_csv(path)
        schema, features = \
            _detect_schema_and_feature(entity,  owner, id_column,
                                       feature_columns, timestamp_column,
                                       timestamp_value, serving_store,
                                       warehouse_store, df)
        iport_spec = _create_import(src_type, source_options, job_options,
                                    entity, schema)

        props = (_properties(src_type, len(df.index), require_staging,
                             source_options["path"]))
        specs = _specs(iport_spec, Entity(name=entity), features)

        return cls(specs, df, props)

    @classmethod
    def from_bq(cls,
                bq_path,
                entity,
                owner,
                limit=10,
                id_column=None,
                feature_columns=None,
                timestamp_column=None,
                timestamp_value=None,
                serving_store=None,
                warehouse_store=None,
                job_options={}):
        """Creates an importer from a given bigquery table. 
        
        Args:
            bq_path (str): path to bigquery table, in the format 
                            project.dataset.table
            entity (str): entity id
            owner (str): owner
            limit (int, optional): Defaults to 10. The maximum number of rows to 
                read into the importer df.
            id_column (str, optional): Defaults to None. Id column in the csv. 
                If not set, will default to the `entity` argument.
            feature_columns ([str], optional): Defaults to None. Feature columns
                to ingest. If not set, the importer will by default ingest all 
                available columns.
            timestamp_column (str, optional): Defaults to None. Timestamp 
                column in the csv. If not set, defaults to timestamp value.
            timestamp_value (datetime, optional): Defaults to current datetime. 
                Timestamp value to assign to all features in the dataset.
            serving_store (feast.sdk.resources.feature.DataStore): Defaults to None.
                Serving store to write the features in this instance to.
            warehouse_store (feast.sdk.resources.feature.DataStore): Defaults to None.
                Warehouse store to write the features in this instance to.
            job_options (dict): Defaults to empty dict. Additional job options.
        
        Returns:
            Importer: the importer for the dataset provided.
        """

        cli = bigquery.Client()
        project, dataset_id, table_id = bq_path.split(".")
        dataset_ref = cli.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)
        table = cli.get_table(table_ref)

        source_options = {
            "project": project,
            "dataset": dataset_id,
            "table": table_id
        }
        df = head(cli, table, limit)
        schema, features = \
            _detect_schema_and_feature(entity,  owner, id_column,
                                       feature_columns, timestamp_column,
                                       timestamp_value, serving_store,
                                       warehouse_store, df)
        iport_spec = _create_import("bigquery", source_options, job_options,
                                    entity, schema)

        props = _properties("bigquery", table.num_rows, False, None)
        specs = _specs(iport_spec, Entity(name=entity), features)
        return cls(specs, df, props)

    @classmethod
    def from_df(cls,
                df,
                entity,
                owner,
                staging_location,
                id_column=None,
                feature_columns=None,
                timestamp_column=None,
                timestamp_value=None,
                serving_store=None,
                warehouse_store=None,
                job_options={}):
        """Creates an importer from a given pandas dataframe. 
        To import a file from a dataframe, the data will have to be staged.
        
        Args:
            path (str): path to csv file
            entity (str): entity id
            owner (str): owner
            staging_location (str): Defaults to None. Staging location 
                                                for ingesting a local csv file.
            id_column (str, optional): Defaults to None. Id column in the csv. 
                                If not set, will default to the `entity` argument.
            feature_columns ([str], optional): Defaults to None. Feature columns
                to ingest. If not set, the importer will by default ingest all 
                available columns.
            timestamp_column (str, optional): Defaults to None. Timestamp 
                column in the csv. If not set, defaults to timestamp value.
            timestamp_value (datetime, optional): Defaults to current datetime. 
                Timestamp value to assign to all features in the dataset.
            serving_store (feast.sdk.resources.feature.DataStore): Defaults to None.
                Serving store to write the features in this instance to.
            warehouse_store (feast.sdk.resources.feature.DataStore): Defaults to None.
                Warehouse store to write the features in this instance to.
            job_options (dict): Defaults to empty dict. Additional job options.
        
        Returns:
            Importer: the importer for the dataset provided.
        """
        tmp_file_name = ("tmp_{}_{}.csv".format(entity,
                                                int(round(
                                                    time.time() * 1000))))
        src_type = "file.csv"
        source_options = {}
        source_options["path"], require_staging = (_get_remote_location(
            tmp_file_name, staging_location))
        schema, features = \
            _detect_schema_and_feature(entity,  owner, id_column,
                                       feature_columns, timestamp_column,
                                       timestamp_value, serving_store,
                                       warehouse_store, df)
        iport_spec = _create_import(src_type, source_options, job_options,
                                    entity, schema)

        props = _properties("dataframe", len(df.index), require_staging,
                            source_options["path"])
        specs = _specs(iport_spec, Entity(name=entity), features)

        return cls(specs, df, props)

    def stage(self):
        """Stage the data to its remote location
        """
        if not self.require_staging:
            return
        ts_col = self.spec.schema.timestampColumn
        _convert_timestamp(self.df, ts_col)
        df_to_gcs(self.df, self.remote_path)

    def describe(self):
        """Print out the import spec.
        """
        print(spec_to_yaml(self.spec))

    def dump(self, path):
        """Dump the import spec to the provided path
        
        Arguments:
            path (str): path to dump the spec to
        """

        with open(path, 'w') as f:
            f.write(spec_to_yaml(self.spec))
        print("Saved spec to {}".format(path))


def _convert_timestamp(df, timestamp_col):
    """Converts the given df's timestamp column to ISO8601 format
    """
    df[timestamp_col] = pd.to_datetime(df[timestamp_col]).dt \
        .strftime("%Y-%m-%dT%H:%M:%S%zZ")


def _properties(source, size, require_staging, remote):
    """Args:
        source (str): source of the data
        size (int): number of rows of the dataset 
        require_staging (bool): whether the file requires staging
        remote (str): remote path 
    
    Returns:
        dict: set of importer properties
    """

    return {
        "source": source,
        "size": size,
        "require_staging": require_staging,
        "remote_path": remote
    }


def _specs(iport, entity, features):
    """Args:
        iport {} -- [description]
        entity {[type]} -- [description]
        features {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """

    return {"import": iport, "features": features, "entity": entity}


def _get_remote_location(path, staging_location):
    """Get the remote location of the file
    
    Args:
        path (str): raw path of the file
        staging_location (str): path to stage the file

    """
    if is_gs_path(path):
        return path, False

    if staging_location is None:
        raise ValueError(
            "Specify staging_location for importing local file/dataframe")
    if not is_gs_path(staging_location):
        raise ValueError("Staging location must be in GCS")

    filename = ntpath.basename(path)
    return staging_location + "/" + filename, True


def _detect_schema_and_feature(entity, owner, id_column, feature_columns,
                               timestamp_column, timestamp_value,
                               serving_store, warehouse_store, df):
    """Create schema object for import spec.
    
    Args:
        entity (str): entity name
        id_column (str): column name of entity id
        timestamp_column (str): column name of timestamp
        timestamp_value (datetime.datetime): timestamp to apply to all 
            rows in dataset
        feature_columns (str): list of column to be extracted
        df (pandas.Dataframe): pandas dataframe of the data
        serving_store (feast.sdk.resources.feature.DataStore): Defaults to None.
            Serving store to write the features in this instance to.
        warehouse_store (feast.sdk.resources.feature.DataStore): Defaults to None.
            Warehouse store to write the features in this instance to.

    Returns:
        feast.specs.ImportSpec_pb2.Schema: schema of the data
        dict of str: feast.specs.FeatureSpec_pb2.FeatureSpec: features in the data

    Raises:
        Exception -- [description]
    """

    schema = Schema()
    if id_column is not None:
        schema.entityIdColumn = id_column
    elif entity in df.columns:
        schema.entityIdColumn = entity
    else:
        raise ValueError("Column with name {} is not found".format(entity))

    if timestamp_column is not None:
        schema.timestampColumn = timestamp_column
    else:
        if timestamp_value is None:
            ts = Timestamp()
            ts.GetCurrentTime()
        else:
            ts = Timestamp(
                seconds=int((timestamp_value -
                             datetime.datetime(1970, 1, 1)).total_seconds()))
        schema.timestampValue.CopyFrom(ts)

    features = {}
    if feature_columns is not None:
        # check if all column exist and create feature accordingly
        for column in feature_columns:
            if column not in df.columns:
                raise ValueError(
                    "Column with name {} is not found".format(column))
            features[column] = _create_feature(df[column], entity, owner,
                                               serving_store, warehouse_store)
    else:
        # get all column except entity id and timestampColumn
        feature_columns = list(df.columns.values)
        _remove_safely(feature_columns, schema.entityIdColumn)
        _remove_safely(feature_columns, schema.timestampColumn)
        for column in feature_columns:
            features[column] = _create_feature(df[column], entity, owner,
                                               serving_store, warehouse_store)

    for col in df.columns:
        field = schema.fields.add()
        field.name = col
        if col in features:
            field.featureId = features[col].id

    features_dict = {}
    for k in features:
        features_dict[features[k].id] = features[k]

    return schema, features_dict


def _create_feature(column, entity, owner, serving_store, warehouse_store):
    """Create Feature object. 
    
    Args:
        column (pandas.Series): data column
        entity (str): entity name
        owner (str): owner of the feature
        serving_store (feast.sdk.resources.feature.DataStore): Defaults to None.
            Serving store to write the features in this instance to.
        warehouse_store (feast.sdk.resources.feature.DataStore): Defaults to None.
            Warehouse store to write the features in this instance to.
    
    Returns:
        feast.sdk.resources.Feature: feature for this data column
    """

    feature = Feature(
        name=column.name,
        entity=entity,
        owner=owner,
        value_type=dtype_to_value_type(column.dtype))
    if serving_store is not None:
        feature.serving_store = serving_store
    if warehouse_store is not None:
        feature.warehouse_store = warehouse_store
    return feature


def _create_import(import_type, source_options, job_options, entity, schema):
    """Create an import spec.
    
    Args:
        import_type (str): import type
        source_options (dict): import spec source options
        jobOptions (dict): import spec job options
        entity (str): entity
        schema (feast.specs.ImportSpec_pb2.Schema): schema of the file
    
    Returns:
        feast.specs.ImportSpec_pb2.ImportSpec: import spec
    """

    return ImportSpec(
        type=import_type,
        sourceOptions=source_options,
        jobOptions=job_options,
        entities=[entity],
        schema=schema)


def _remove_safely(columns, column):
    try:
        columns.remove(column)
    except ValueError:
        pass
