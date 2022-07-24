# File

## Description

File data sources allow for the retrieval of historical feature values from files on disk for building training datasets, as well as for materializing features into an online store.

{% hint style="warning" %}
FileSource is meant for development purposes only and is not optimized for production use.
{% endhint %}

## Example

```python
from feast import FileSource
from feast.data_format import ParquetFormat

parquet_file_source = FileSource(
    file_format=ParquetFormat(),
    path="file:///feast/customer.parquet",
)
```

Configuration options are available [here](https://rtd.feast.dev/en/latest/index.html#feast.data_source.FileSource).

