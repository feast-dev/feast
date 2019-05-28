- Feature Name: python_sdk
- Created Date: 2018-12-24
- RFC PR: [#47](https://github.com/gojek/feast/pull/47)
- Feast Issue: [#48](https://github.com/gojek/feast/issues/48)

# Summary
[summary]: #summary

The Feast Python SDK is a library used to interact with Feast from Python. It aims to allow users to manage features and entities, generate and register specifications, as well as load or retrieve feature data.

# Motivation
[motivation]: #motivation

Python is one of the primary ways in which data scientists explore and develop data and models. Even though Feast is intended to be used by data scientists in their projects, there is currently no way to do this from within a Python development environment. It would be very useful to have a way to interact with Feast from Python:

* Python is typically used for exploration and prototyping at the start of ML projects, which is also where a feature store is needed. Allowing users to use Feast without leaving the their development environment would be very helpful.
* Most model training and evaluation steps are executed from within Python. By having Feast available as a Python library, we would be able to simply extend the steps before training to retreive feature data.
* ML practitioners are generally more comfortable with Python than they are with Bash and other programming languages.
* The adoption and readability makes producing Feast examples a lot easier. It would be possible to simply insert Feast code into existing Python notebooks to showcase its usage.

# Guide-level explanation
[guide-level-explanation]: #guide-level-explanation

The Feast Python SDK allows you to take the following actions

* Connect to an existing Feast deployment
* Create & register entities, features, imports, and data stores
* Generate import specifications from BigQuery tables or Pandas dataframes
* Start import jobs from import specifications to load data into Feast
* Export objects into specification format for re-use
* Retrieve training data from Feast for training models
* Retrieve serving data from Feast for inference or other use cases

## Python SDK examples

What follows is an example of how the Python SDK can be used by the end user.

### Connect to your Feast server

```python
fs = client('my.feast.server')
```

### Create new customer entity

```python
customer_entity = Entity('customer', "desc", tags=["loyal", "customer"])
```

### Create customer features

```python

customer_age = Feature(name='age', 
                       entity="customer",
                       owner='user@website.com',
                       description="Customer's age",
                       value_type=ValueType.INT64, 
                       serving_store=Datastore(id="REDIS1"),
                       warehouse_store=Datastore(id="BIGQUERY1"))

customer_balance = Feature(name='balance', 
                           entity="customer",
                           owner='user@website.com', 
                           value_type=ValueType.FLOAT, 
                           description="Customer's account balance",
                           serving_store=Datastore(id="REDIS1"),
                           warehouse_store=Datastore(id="BIGQUERY1"))
```

### Register customer entity in Feast using the apply method

```python
fs.apply(customer_entity)
```

The apply method is idempotent. It allows you to submit one or more resources to Feast. If the command is rerun, Feast will apply only the change. 

### Register customer's "age" feature in Feast using the apply method

```python
fs.apply(customer_age)
```

### Register multiple customer features

```python
fs.apply([customer_age, customer_balance])
```

### Register customer's "age" feature in Feast with "create" command

```python
fs.create(customer_age)
```

If the user uses the `create` method and the feature `customer_age` already exists, then failure will occur. `create` is identical to `apply`, except that it does not allow resources to already exist within Feast.

### Create an importer from a csv
Importers allow for the creation of jobs to ingest feature data into Feast. Importers are generally (but not necessarily) created from existing data sets (BQ, CSV, Pandas dataframe) in order to discover feature names.

```python
cust_importer = Importer.from_csv('customer_features.csv', 
                                    entity='customer', 
                                    owner='user@website.com',
                                    staging_location="gs://my-bucket/feast",
                                    id_column="customer_id", 
                                    timestamp_value=datetime.datetime.now())
```

### [Alternative] Create an importer from a BigQuery table

```python
driver_importer_from_bq = Importer.from_bq("my_project.dataset1.table1", 
                                           entity="s2id", 
                                           owner='user@website.com',
                                           timestamp_column="start_time")
```

### Describe the importer

```python
driver_importer_from_bq.describe()
for feat in driver_importer_from_bq.features:
    print(feat)
```

Output:
```markdown
type: bigquery
options:
  dataset: dataset1
  project: my_project
  table: table1
entities:
- s2id
schema:
  entityIdColumn: s2id
  fields:
  - name: start_time
  - name: s2id
  - featureId: s2id.surge_factor
    name: surge_factor
  timestampColumn: start_time

id: s2id.surge_factor
name: surge_factor
owner: user@website.com
valueType: DOUBLE
entity: s2id
dataStores: {}
```

### [Alternative] Create an importer from a Pandas dataframe

```python
my_pandas_df = pd.read_csv("driver_features.csv")
driver_importer_from_df = Importer.from_df(my_pandas_df, 
                                           entity='driver', 
                                           owner='user@website.com',  
                                           staging_location="gs://staging-bucket/feast",
                                           id_column="driver_id", 
                                           timestamp_column="ts")
```

### Preview the dataframe loaded by the importer

The `head` method prints out the first 5 rows of the Pandas dataframe.

```python
cust_importer.df.head()
```

### Change the local file path in the importer to GCS

In order to ingest data from a bucket, it is necessary to modify the importer's path configuration to the location of the file that needs to be imported.

```python
driver_importer.options.path = 'gs://my-bucket-location/driver_features.csv'
```

### Submit the import job

The import job loads the CSV from GCS into Feast. It automatically registers entities and features with Feast during submission.

```python
fs.run(cust_importer, name_override="myjob")
```
_starting import myjob1545303576252..._  
_10%_   
_50%_   
_100%_    
_1000 feature rows imported successfully_ 

### Write out specification files for later use

```python
cust_importer.dump("driver_feature_import.yaml")
customer_entity.dump("customer_entity.yaml")
customer_age.dump("customer_entity.yaml")
```

### Create a “feature set” which can be used to query both training data and serving data.

The feature set is simply an object that locally tracks which entity, and features you are interested in.

```python
driver_feature_set = FeatureSet(entity='driver', features=['latitude', 'longitude', 'event_time'])
```

### Produce training dataset

* Stages a table in BQ with output data
* Returns information about the dataset that has been created

```python
driver_dataset_info = fs.create_training_dataset(
  driver_feature_set,
  start_date='2018-01-01', 
  end_date='2018-02-01')
```

### Retrieve training dataset using CSV

```python
file_path = 'driver_features.csv'
fs.download_dataset(driver_dataset_info, dest=file_path, type='csv')
```

### Load training dataset into Pandas

```python
import pandas as pd
df = pd.read_csv(file_path)
```

### [Alternative] Download dataset directly into a Pandas dataframe

```python
driver_df = fs.download_dataset_to_df(driver_dataset_info)
```

### Train model
The user can now split their dataset and train their model. This is out of scope for the SDK.

### Running inference
Ensure you have the list of entity keys for which you want to retrieve features

```python
keys = [12345, 67890]
```

Fetch serving data from Feast by reusing the same feature set

```python
feature_data = fs.get_serving_data(driver_feature_set, keys, type='last')
```

```
output = model.predict(feature_data)
```

## Impact

From a user's perspective, the SDK allows them to stay within Python to execute most of the commands that they would need to in order to manage Feast.
* Users can define features and load data from their data processing pipelines
* Users can retrieve features for training their models in their model training pipelines
* Users can quickly query Feast for serving feature data to validate whether serving features are consistent with training.
* Users can programmatically interact with Feast from within Python, which could would be useful for managing entities, features, imports, or interacting with existing feature data.
* Overall the goal is that time to market decreases, because users are able to accelerate their development loop.

# Reference-level explanation
[reference-level-explanation]: #reference-level-explanation

Users of the SDK will use the following classes when interacting with Feast

__Resource classes__

These classes allow users to describe and register their resources (entities, features, data stores) with Feast.

* __Entity__: Represents an entity within Feast.
* __Feature__: Represents a single feature within Feast.
* __FeatureSet__: Represents a grouping of features within Feast (for retrieval).
* __Storage__: Represents a data store that Feast will use to store features.

__Management classes__

These classes allow users to manage resources and jobs.

* __Importer__: Used to load feature data into Feast.
* __Client__: The Feast client is the means by which resources, jobs, and data is managed.

This reference level explanation describes some of the important underlying classes and methods of the SDK in more detail.

### Class: Entity

__Method__: `__init__`

Initializes an `entity` during instantiation.

```
Args:
  name (str): name of entity
  description (str): description of entity
  tags (list[str], optional): defaults to []. 
      list of tags for this entity
```

__Method__: `from_yaml`

Create an instance of an `entity` from a yaml file.
```
Args:
  path (str): path to yaml file
```

__Method__: `dump`

Export the contents of this `entity` to a yaml file.

```
Args:
  path (str): destination file path
```
### Class: Feature

__Method__: `__init__`

Initializes a `feature` during instantiation.

```
Args:
  name (str): name of feature, in lower snake case
  entity (str): entity the feature belongs to, in lower case
  owner (str): owner of the feature
  value_type (feast.types.ValueType_pb2.ValueType): defaults to ValueType.DOUBLE. value type of the feature
  description (str): defaults to "". description of the feature
  uri (str): defaults to "". uri pointing to the source code or origin of this feature
  warehouse_store (feast.specs.FeatureSpec_pb2.Datastore): warehouse store id and options
  serving_store (feast.specs.FeatureSpec_pb2.Datastore): serving store id and options
  group (str, optional): feature group to inherit from
  tags (list[str], optional): tags assigned to the feature
  options (dic, optional): additional options for the feature
```

__Method__: `from_yaml`

Create an instance of a `feature` from a yaml file.
```
Args:
  path (str): path to yaml file
```

__Method__: `dump`

Export the contents of this `feature` to a yaml file.

```
Args:
  path (str): destination file path
```

### Class: Importer

__Method__: `from_csv`

Creates an `importer` from a given csv dataset. This file can be either local or remote (in gcs). If it's a local file then staging_location must be specified.

```
Args:
  path (str): path to csv file
  entity (str): entity id
  owner (str): owner
  staging_location (str, optional): Defaults to None. Staging location for ingesting a local csv file.
  id_column (str, optional): Defaults to None. Id column in the csv. If not set, will default to the `entity` argument.
  feature_columns ([str], optional): Defaults to None. Feature columns to ingest. If not set, the importer will by default ingest all available columns.
  timestamp_column (str, optional): Defaults to None. Timestamp column in the csv. If not set, defaults to timestamp value.
  timestamp_value (datetime, optional): Defaults to current datetime. Timestamp value to assign to all features in the dataset.

Returns:
  Importer: the importer for the dataset provided.
```

__Method__: `from_bq`

Creates an `importer` from a given BigQuery table. 

```
Args:
  path (str): path to csv file
  entity (str): entity id
  owner (str): owner
  staging_location (str, optional): Defaults to None. Staging location for ingesting a local csv file.
  id_column (str, optional): Defaults to None. Id column in the csv. If not set, will default to the `entity` argument.
  feature_columns ([str], optional): Defaults to None. Feature columns to ingest. If not set, the importer will by default ingest all available columns.
  timestamp_column (str, optional): Defaults to None. Timestamp column in the csv. If not set, defaults to timestamp value.
  timestamp_value (datetime, optional): Defaults to current datetime. Timestamp value to assign to all features in the dataset.

Returns:
  Importer: the importer for the dataset provided.
```

__Method__: `from_df`

Creates an importer from a given pandas dataframe. To import a file from a dataframe, the data must be staged locally.

```
Args:
  path (str): path to csv file
  entity (str): entity id  
  owner (str): owner
  staging_location (str, optional): Defaults to None. Staging location for ingesting a local csv file.
  id_column (str, optional): Defaults to None. Id column in the csv. If not set, will default to the `entity` argument.
  feature_columns ([str], optional): Defaults to None. Feature columns to ingest. If not set, the importer will by default ingest all available columns.
  timestamp_column (str, optional): Defaults to None. Timestamp column in the csv. If not set, defaults to timestamp value.
  timestamp_value (datetime, optional): Defaults to current datetime. Timestamp value to assign to all features in the dataset.

Returns:
  Importer: the importer for the dataset provided.
```

__Method__: `describe`

Prints out the properties of the `importer` as an import specification.

__Method__: `dump`

Saves the `importer` as an import specification to the local file system.
```
  Args:
    path (str): path to dump the spec to
```

### Class: Client

__Method__: `__init__`

Initializes a Feast client instance which immediately connects to the Feast deployment specified. If no url is provided, the client will default to the URL specified in the environmental variable `FEAST_CORE_URL`.

```
Args:
  core_url (str, optional): feast's grpc endpoint URL
                        (e.g.: "my.feast.com:8433")
  serving_url (str, optional): feast serving's grpc endpoint URL
                        (e.g.: "my.feast.com:8433")
```

__Method__: `apply`

Create or update one or more Feast resources (feature, entity, importer, storage). This method is idempotent. It can be rerun without any side-effects, as long as the user does not attempt to change immutable properties of resources.

```
Args:
  obj (object): one or more feast resources
```

__Method__: `create`

Create one or more Feast resources (feature, entity, importer, storage). This method will return an error of the resource already exists.

```
Args:
  obj (object): one or more feast resources
```

__Method__: `run`

Submits an Importer to Feast to start an import operation of Feature data.

```
Args:
  importer (object): Instance of an importer to run
  name_override (str, optional): Set name of import Job
  apply_entity (bool, optional): Run apply for entities in importer
  apply_features (bool, optional): Run apply for all features in importer
```
            
__Method__: `close`

Closes the connection to a Feast instance.

__Method__: `get_serving_data`

Retrieve data from the feast serving layer. You can either retrieve the the latest value, or a list of the latest values, up to a provided limit. If `server_url` is not provided, the value stored in the environment variable FEAST_SERVING_URL is used to connect to the serving server instead.

```
Args:
  feature_set (feast.sdk.resources.feature_set.FeatureSet): feature set representing the data wanted
  entity_keys (:obj: `list` of :obj: `str): list of entity keys
  request_type (feast.sdk.utils.types.ServingRequestType):
      (default: feast.sdk.utils.types.ServingRequestType.LAST) type of request: one of [LIST, LAST]
  ts_range (:obj: `list` of :obj: `datetime.datetime`, optional): size 2 list of start timestamp and end timestamp. Only required if request_type is set to LIST
  limit (int, optional): (default: 10) number of values to get. Only required if request_type is set to LIST
  
Returns:
  pandas.DataFrame: DataFrame of results
```

__Method__: `download_dataset_to_df`

Downloads a dataset into a Pandas dataframe.

```
Args:
  dataset (Feast.DataSet): An instance of the DataSet object which is returns from the `create_training_data` method.
Returns:
  pandas.DataFrame: DataFrame of feature data
```

__Method__: `download_dataset`

Downloads a dataset into a local file.

```
Args:
  dataset (Feast.DataSet): An instance of the DataSet object which is returns from the `create_training_data` method.
  dest (str): location to store the file locally.
  type: format to store dataset as (csv, avro)
```

# Drawbacks
[drawbacks]: #drawbacks

The primary drawback of using a programming language to interact with and manage Feast, is that it allows users to produce code that is harder to understand than simply YAML configuration (which is what Feast uses currently). For example, users would be able to use Jinja templating to produce their Feast resources, which would decrease readability.

# Rationale and alternatives
[rationale-and-alternatives]: #rationale-and-alternatives

Why is this design the best in the space of possible designs?

The design contains a minimal set of functionalities that already exist in Feast, mapped over into Python from the CLI. It does not introduce any new functionality, with the exception of "Feature Sets" and the retrieval of batch feature data. The SDK simply allows access to the functionality from Python.

What other designs have been considered and what is the rationale for not choosing them?

Other languages have been considered, but none compare to Python for this use case. This version of the SDK is a first cut, and much of the higher order functionality like `feature sets` and `data sets` are left in a basic state.

What is the impact of not doing this?

* It would be very difficult to onboard new users onto Feast without having a Python example to show them.
* Frustration and development time would increase if users are asked to interact with Feast from the command line or a user interface.
* Project code would be less consistent and readable if multiple development languages are used.

# Prior art
[prior-art]: #prior-art

No prior art was found for this RFC.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

What parts of the design do you expect to resolve through the RFC process before this gets merged?

* The handling of feature sets, data sets, job execution, and arguments used for methods.

What parts of the design do you expect to resolve through the implementation of this feature before stabilization?

* N/A

What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?

* N/A

# Future possibilities
[future-possibilities]: #future-possibilities

The Feast Python SDK is a natural iteration in making Feast more user friendly. The first version will only have a minimal API, but the SDK can become the base for adding new functionality to Feast.
