- Feature Name: python_sdk
- Created Date: 2018-12-24
- RFC PR:
- Feast Issue:

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
customer_entity = entity('customer')
```

### Create customer features

```python
customer_age = feature(name='age', granularity=granularity.DAY, owner='owner@email.com', valueType=valueType.INT64, description='Customer\'s age')
customer_balance = feature(name='balance', granularity=granularity.DAY, owner='owner@email.com', valueType=valueType.FLOAT, description='Customer\'s account balance')
```

### Register customer entity in Feast using the apply method

```python
fs.apply(customer_entity)
```

The apply method is idempotent. It allows you to submit one or more resources to Feast. If the command is rerun, Feast will apply only the difference. 

### Register multiple customer features

```python
fs.apply([customer_age, customer_balance])
```

### Register customer's "age" feature in Feast using the apply method

```python
fs.apply(customer_age)
```

### Register customer's "age" feature in Feast with "create" command

```python
fs.create(customer_age)
```
If the user uses the `create` method and the feature `customer_age` already exists, then failure will occur. `create` is identical to `apply`, except that it does not allow resources to already exist within Feast.

### Create an importer from a csv
Importers allow for the creation of jobs to ingest feature data into Feast. Importers are generally (but not necessarily) created from existing data sets (BQ, CSV, Pandas dataframe) in order to discover feature names.

```python
driver_importer = importer.from_csv('driver_features.csv', granularity=granularity.DAY, entity='driver', owner='owner@email.com')
```

### [Alternative] Create an importer from a BigQuery table

```python
driver_importer_from_bq = importer.from_bq('project.dataset.drivers_table', granularity=granularity.DAY, entity='driver', owner='owner@email.com')
```

### [Alternative] Create an importer from a Pandas dataframe

```python
my_pandas_df = pd.read_csv('driver_features.csv')
driver_importer_from_df = importer.from_df(my_pandas_df, granularity=granularity.DAY, entity='driver', owner='owner@email.com')
```

### Preview the dataframe loaded by the importer
The `head` method prints out the first 5 rows of the Pandas dataframe.

```python
driver_importer.df.head()
```

### Change the local file path in the importer to GCS
In order to ingest data from a bucket, it is necessary to modify the importer's path configuration to the location of the file that needs to be imported.

```python
driver_importer.options.path = 'gs://my-bucket-location/driver_features.csv'
```

### Describe the importer

```python
driver_importer.describe()
```

```yml
type: file
options:
  format: csv
  path: gs://my-bucket-location/driver_features.csv
entities:
  - driver
schema:
  entityIdColumn: driver_id
  timestampColumn: ts
  fields:
    - name: timestamp
    - name: customer_id
    - name: completed
      featureId: completed
    - name: avg_distance_completed
      featureId: avg_distance_completed
    - name: avg_customer_distance_completed
      featureId: avg_customer_distance_completed
    - name: avg_distance_cancelled
      featureId: avg_distance_cancelled
```

### Submit the import job
The import job loads the CSV from GCS into Feast. It automatically registers entities and features with Feast during submission.

```python
fs.apply(driver_importer, create_entity=True, create_features=True)
```
_starting import..._  
_10%_   
_50%_   
_100%_    
_10 rows imported successfully_ 

### Write out specification files for later use

```python
driver_importer.dump("driver_feature_import.yaml")
customer_entity.dump("customer_entity.yaml")
customer_age.dump("customer_entity.yaml")
```

### Create a “feature set” which can be used to query both training data and serving data.
The feature set is simply an object that locally tracks which entity, granularity, and features you are interested in.

```python
feature_set = fs.create_feature_set(entity='driver', granularity='minute', features=['latitude', 'longitude', 'event_time'])
```

### Produce training dataset

```python
dataset_info = feature_set.create_training_dataset(start_date='2018-01-01', end_date='2018-02-01')
```

### Retrieve training dataset

```python
file_path = 'mypath.feather'
dataset_info.download(destination=file_path, type='feather')
```

### Load training dataset into Pandas

```python
import feather
df = feather.read_dataframe(file_path)
```

### [Alternative] Download dataset directly into a Pandas dataframe

```python
df = dataset_info.download_to_df()
```

### Train the model
The user can now split their dataset and train their model. This is out of scope for the SDK.

### Running inference
Ensure you have the list of entity keys for which you want to retrieve features

```python
keys = [12345, 67890]
```

Fetch serving data from Feast by reusing the same feature set

```python
feature_data = feature_set.get_serving_data(keys, type='last')
```


## Impact

From a user's perspective, the SDK allows them to stay within Python to execute most of the commands that they would need to in order to manage Feast.
* They can define features and load data from their data processing pipelines
* They can retrieve features for training their models in their model training pipelines
* They can quickly query Feast for serving feature data to validate whether serving features are consistent with training.
* They can programmatically interact with Feast from within Python, which could would be useful for managing entities, features, imports, or interacting with existing feature data.
* Overall the hope is that time to market decreases, because users are able to accelerate the development loop.

# Reference-level explanation
[reference-level-explanation]: #reference-level-explanation



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
