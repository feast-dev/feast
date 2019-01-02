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

import itertools
import operator
import os
import pandas as pd

from google.cloud.bigquery.client import Client as BQClient
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.bigquery.job import QueryJobConfig
from jinja2 import Template


def head(client, table, max_rows=10):
    '''Get the head of the table. Retrieves rows from the given table at 
        minimum cost
    
    Args:
        client (google.cloud.bigquery.client.Client): bigquery client
        table (google.cloud.bigquery.table.Table): bigquery table to get the 
            head of
        max_rows (int, optional): Defaults to 10. maximum number of rows to 
            retrieve
    
    Returns:
        pandas.DataFrame: dataframe containing the head of rows
    '''
    
    rows = client.list_rows(table, max_results=max_rows)
    rows = [x for x in rows]
    return pd.DataFrame(
        data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))


def get_table_name(feature_id, storage_spec):
    """
    Get fully qualified BigQuery table name from a feature ID and its
    storage spec
    Args:
        feature_id(str): ID of a feature
        storage_spec(feast.specs.StorageSpec_pb2.StorageSpec): storage spec of
        the feature

    Returns:
         str: fully qualified table name of the feature.

    """
    if "bigquery" != storage_spec.type:
        raise ValueError("storage spec is not BigQuery storage spec")

    try:
        project = storage_spec.options["project"]
        dataset = storage_spec.options["dataset"]
    except KeyError:
        raise ValueError("storage spec has empty project or dataset option")

    table_name = "_".join(feature_id.split(".")[:2])
    return ".".join([project, dataset, table_name])


class TrainingDatasetCreator:
    """
    Helper class to create a training dataset.
    """

    def __init__(self):
        self._bq = BQClient()
        script_dir = os.path.dirname(__file__)
        with open(os.path.join(script_dir, "template", "training_query.j2")) \
                as file:
            self._sql_template = Template(file.read())

    def create_training_dataset(self, feature_table_tuples,
                                start_date, end_date, limit,
                                destination):
        """
        Create training dataset for given list of feature and its table
        between start_date and end_date.

        Args:
            feature_table_tuples: [(str, str)] list of tuple of
                feature id and it's BQ table ID
            start_date: (str) start date of the training dataset
            end_date: (str) end date of the training dataset
            limit: (int) maximum number of row returned
            destination: (str) fully qualified BigQuery table ID of the
                destination

        Returns: (google.cloud.bigquery.table.Table) Bigquery table instance

        """
        query = self._create_query(feature_table_tuples, start_date,
                                   end_date, limit)
        query_config = QueryJobConfig()
        query_config.destination = TableReference.from_string(destination)

        query_job = self._bq.query(query, query_config)
        # wait until completion
        query_job.result()

        return Table.from_string(destination)

    def _create_query(self, feature_table_tuples, start_date,
                      end_date, limit):
        feature_groups = self._group_features(feature_table_tuples)
        return self._sql_template.render(feature_groups=feature_groups,
                                         start_date=start_date,
                                         end_date=end_date,
                                         limit=limit)

    def _group_features(self, feature_table_tuples):
        feature_groups = []
        # (table_id, granularity, feature id)
        temp = [((table_id, feature_id.split(".")[1]), feature_id)
                for (feature_id, table_id) in feature_table_tuples]
        it = itertools.groupby(temp, operator.itemgetter(0))
        for key, subiter in it:
            features = [_Feature(item[1]) for item in subiter]
            feature_groups.append(_FeatureGroup(key[0], key[1], features))
        # sort by granularity
        feature_groups.sort()
        return feature_groups


class _Feature:
    """
    Helper class for templating a feature in training query
    """
    def __init__(self, feature_id):
        self.name = feature_id.split(".")[2]
        self.column = feature_id.replace(".", "_")


class _FeatureGroup:
    """
    Helper class for templating a group of feature having same table_id and
    granularity in training query
    """
    def __init__(self, table_id, granularity, features):
        self.table_id = table_id
        self.temp_table = table_id.replace(".", "_").replace("-", "_")
        self.granularity = granularity
        self.features = features

    GRANULARITY_SCORE = {
        "second": 0,
        "minute": 1,
        "hour": 2,
        "day": 3,
        "none": 4
    }

    def __lt__(self, other):
        my_score = self.GRANULARITY_SCORE[self.granularity]
        other_score = self.GRANULARITY_SCORE[other.granularity]

        if my_score != other_score:
            return my_score < other_score

        return self.table_id < other.table_id