import pandas as pd
import ntpath
import time
import datetime
from feast.specs.ImportSpec_pb2 import ImportSpec, Schema, Field
from feast.sdk.utils.gs_utils import gs_to_df, is_gs_path, df_to_gs
from feast.sdk.utils.print_utils import spec_to_yaml
from feast.sdk.utils.types import dtype_to_value_type
from feast.sdk.utils.bq_util import head
from feast.sdk.resources.feature import Feature

from google.protobuf.timestamp_pb2 import Timestamp 
from google.cloud import bigquery

class Importer:
    def __init__(self):
        self.source = None
        self.require_staging = False
        self.remote_path = None
        self.spec = None
        self.features = []
        self.df = None
        self.bq_client = None

    @classmethod
    def from_csv(cls, path, entity, granularity, owner, staging_location=None,
        id_column=None, feature_columns=None, timestamp_column=None, 
        timestamp_value=None):
        '''Creates an importer from a given csv dataset. 
        This file can be either local or remote (in gcs). If it's a local file 
        then staging_location must be determined.
        
        Args:
            path (str): path to csv file
            entity (str): entity id
            granularity (Granularity): granularity of data
            owner (str): owner
            staging_location (str, optional): Defaults to None. Staging location 
                                                for ingesting a local csv file.
            id_column (str, optional): Defaults to None. Id column in the csv. 
                                If not set, will default to the `entity` argument.
            feature_columns ([str], optional): Defaults to None. Feature columns
                                to ingest. If not set, the importer will by default
                                ingest all available columns.
            timestamp_column (str, optional): Defaults to None. Timestamp 
                    column in the csv. If not set, defaults to timestamp
                    value.
            timestamp_value (datetime, optional): Defaults to current datetime. 
                    Timestamp value to assign to all features in the dataset.
        
        Returns:
            Importer: the importer for the dataset provided.
        '''
        import_spec_options = {"format": "csv"}
        import_spec_options["path"], require_staging = _get_remote_location(path, 
                                                            staging_location)
        iport = cls.__new__(cls)
        iport.source = "csv"
        iport.require_staging = require_staging
        iport.remote_path = import_spec_options["path"]

        if is_gs_path(path):
            iport.df = gs_to_df(path)
        else:
            iport.df = pd.read_csv(path)

        schema, iport.features = _detect_schema_and_feature(entity, 
            granularity, owner, id_column, feature_columns, timestamp_column, 
            timestamp_value, iport.df)
        iport.spec = _create_import("file", import_spec_options, entity, schema)
        return iport
    
    @classmethod
    def from_bq(cls, bq_path, entity, granularity, owner, limit=10, 
        id_column=None, feature_columns=None, timestamp_column=None,
        timestamp_value=None):
        '''Creates an importer from a given bigquery table. 
        
        Args:
            bq_path (str): path to bigquery table, in the format 
                            project.dataset.table
            entity (str): entity id
            granularity (Granularity): granularity of data
            owner (str): owner
            limit (int, optional): Defaults to 10. The maximum number of rows to 
                                read into the importer df.
            id_column (str, optional): Defaults to None. Id column in the csv. 
                                If not set, will default to the `entity` argument.
            feature_columns ([str], optional): Defaults to None. Feature columns
                                to ingest. If not set, the importer will by default
                                ingest all available columns.
            timestamp_column (str, optional): Defaults to None. Timestamp 
                    column in the csv. If not set, defaults to timestamp
                    value.
            timestamp_value (datetime, optional): Defaults to current datetime. 
                    Timestamp value to assign to all features in the dataset.
        
        Returns:
            Importer: the importer for the dataset provided.
        '''
        iport = cls.__new__(cls)
        iport.source = "bigquery"
        iport.bq_client = bigquery.Client()
        project, dataset_id, table_id = bq_path.split(".")
        dataset_ref = iport.bq_client.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)
        table = iport.bq_client.get_table(table_ref)
        
        import_spec_options = {
            "project": project,
            "dataset": dataset_id,
            "table": table_id
        }

        iport.require_staging = False
        iport.df = head(iport.bq_client, table, limit)

        schema, iport.features = _detect_schema_and_feature(entity, 
            granularity, owner, id_column, feature_columns, timestamp_column, 
            timestamp_value, iport.df)

        iport.spec = _create_import("bigquery", import_spec_options, entity, schema)
        return iport
    
    @classmethod
    def from_df(cls, df, entity, granularity, owner, staging_location,
        id_column=None, feature_columns=None, timestamp_column=None,
        timestamp_value=None):
        '''Creates an importer from a given pandas dataframe. 
        To import a file from a dataframe, the data will have to be staged.
        
        Args:
            path (str): path to csv file
            entity (str): entity id
            granularity (Granularity): granularity of data
            owner (str): owner
            staging_location (str): Defaults to None. Staging location 
                                                for ingesting a local csv file.
            id_column (str, optional): Defaults to None. Id column in the csv. 
                                If not set, will default to the `entity` argument.
            feature_columns ([str], optional): Defaults to None. Feature columns
                                to ingest. If not set, the importer will by default
                                ingest all available columns.
            timestamp_column (str, optional): Defaults to None. Timestamp 
                    column in the csv. If not set, defaults to timestamp
                    value.
            timestamp_value (datetime, optional): Defaults to current datetime. 
                    Timestamp value to assign to all features in the dataset.
        
        Returns:
            Importer: the importer for the dataset provided.
        '''
        tmp_file_name = "tmp_{}_{}.csv".format(entity, int(round(time.time() * 1000)))
        iport = cls.__new__(cls)
        iport.source = "dataframe"
        iport.remote_path, iport.require_staging = _get_remote_location(tmp_file_name, staging_location)
        iport.df = df
        import_spec_options = {
            "format": "csv",
            "path": iport.remote_path
        }

        schema, iport.features = _detect_schema_and_feature(entity, 
            granularity, owner, id_column, feature_columns, timestamp_column, 
            timestamp_value, iport.df)
        iport.spec = _create_import("file", import_spec_options, entity, schema)
        return iport

    def stage(self):
        '''Stage the data to its remote location
        '''

        if not self.require_staging:
            return
        df_to_gs(self.df, self.remote_path)

    def describe(self):
        '''Print out the import spec.
        '''
        print(spec_to_yaml(self.spec))

    def dump(self, path):
        '''Dump the import spec to the provided path
        
        Arguments:
            path (str): path to dump the spec to
        '''

        with open(path, 'w') as f:
            f.write(spec_to_yaml(self.spec))
        print("Saved spec to {}".format(path))

def _get_remote_location(path, staging_location):
    '''Get the remote location of the file
    
    Args:
        path {str}: [description]
        staging_location {str}: [description]

    '''
    if (is_gs_path(path)):
        return path, False
    
    if staging_location is None:
        raise ValueError("Specify staging_location for importing local file/dataframe")
    if not is_gs_path(staging_location):
        raise ValueError("Staging location must be in GCS")
    
    filename = ntpath.basename(path)
    return staging_location + "/" + filename, True


def _detect_schema_and_feature(entity, granularity, owner, id_column, 
    feature_columns, timestamp_column, timestamp_value, df):
    '''Create schema object for import spec.
    
    Args:
        entity (str): entity name
        granualrity (Granularity): granularity of the feature
        id_column (str): column name of entity id
        timestamp_column (str): column name of timestamp
        timestamp_value (datetime): timestamp to apply to all rows in dataset
        feature_columns (str): list of column to be extracted
        df (Dataframe): pandas dataframe of the data
    
    Raises:
        Exception -- [description]
    '''

    schema = Schema()
    if (id_column is not None):
        schema.entityIdColumn = id_column
    elif entity in df.columns:
        schema.entityIdColumn = entity
    else:
        raise ValueError("Column with name {} is not found".format(entity))

    if (timestamp_column is not None):
        schema.timestampColumn = timestamp_column
    else:
        if timestamp_value == None: 
            ts = Timestamp()
            ts.GetCurrentTime()
        else:
            ts = Timestamp(seconds=
                int((timestamp_value - datetime.datetime(1970,1,1)).total_seconds()))
        schema.timestampValue.CopyFrom(ts)
    
    features = {}
    if (feature_columns is not None):
        # check if all column exist and create feature accordingly                
        for column in feature_columns:
            if column not in df.columns:
                raise ValueError("Column with name {} is not found".format(column))
            features[column] = _create_feature(df[column], entity, granularity, owner)     
    else:
        # get all column except entity id and timestampColumn
        feature_columns = list(df.columns.values)
        _remove_safely(feature_columns, schema.entityIdColumn)
        _remove_safely(feature_columns, schema.timestampColumn)
        for column in feature_columns:
            features[column] = _create_feature(df[column], entity, granularity, owner)     
    
    for col in df.columns:
        field = schema.fields.add()
        field.name = col
        if col in features:
            field.featureId = features[col].id

    return schema, [features[k] for k in features]

def _create_feature(column, entity, granularity, owner):
    '''Create Feature object. 
    
    Args:
        column (pandas.Series): data column
        entity (str): entity name
        granularity (Granularity.Enum): granularity of the feature
        owner (str): owner of the feature
    
    Returns:
        Feature: feature for this data column
    '''
    return Feature(
        name=column.name,
        entity=entity,
        granularity=granularity,
        owner=owner, 
        value_type=dtype_to_value_type(column.dtype))

def _create_import(import_type, options, entity, schema):
    '''Create an import spec.
    
    Args:
        import_type (str): import type
        options (dict): import spec options
        entity (str): entity
        schema (Schema): schema of the file
    
    Returns:
        ImportSpec: import spec
    '''

    return ImportSpec(
        type=import_type,
        options=options,
        entities=[entity],
        schema=schema)

def _remove_safely(columns, column):
    try:
        columns.remove(column)
    except ValueError:
        pass