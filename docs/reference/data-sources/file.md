# File source

## Description

File data sources are files on disk or on S3.
Currently only Parquet and Delta formats are supported.

## Example

```python
from feast import FileSource
from feast.data_format import ParquetFormat

parquet_file_source = FileSource(
    file_format=ParquetFormat(),
    path="file:///feast/customer.parquet",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/latest/index.html#feast.infra.offline_stores.file_source.FileSource).

## Supported Types

File data sources support all eight primitive types and their corresponding array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
