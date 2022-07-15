This directory contains the Go logic that's executed by the `EmbeddedOnlineFeatureServer` from Python.

## Building and Linking
[gopy](https://github.com/go-python/gopy) generates (and compiles) a CPython extension module from a Go package. That's what we're using here, as visible in [setup.py](../setup.py).

Under the hood, gopy invokes `go build`, and then templates `cgo` stubs for the Go module that exposes the public functions from the Go module as C functions.
For our project, this stuff can be found at `sdk/python/feast/embedded_go/lib/embedded.go` & `sdk/python/feast/embedded_go/lib/embedded_go.h` after running `make compile-go-lib`.

## Arrow memory management
Understanding this is the trickiest part of this integration.

At a high level, when using the Python<>Go integration, the Python layer exports request data into an [Arrow Record batch](https://arrow.apache.org/docs/python/data.html) which is transferred to Go using Arrow's zero copy mechanism.
Similarly, the Go layer converts feature values read from the online store into a Record Batch that's exported to Python using the same mechanics.

The first thing to note is that from the Python perspective, all the export logic assumes that we're exporting to & importing from C, not Go. This is because pyarrow only interops with C, and the fact we're using Go is an implementation detail not relevant to the Python layer.

### Export Entities & Request data from Python to Go
The code exporting to C is this, in [online_feature_service.py](../sdk/python/feast/embedded_go/online_features_service.py)
```
(
    entities_c_schema,
    entities_ptr_schema,
    entities_c_array,
    entities_ptr_array,
) = allocate_schema_and_array()
(
    req_data_c_schema,
    req_data_ptr_schema,
    req_data_c_array,
    req_data_ptr_array,
) = allocate_schema_and_array()

batch, schema = map_to_record_batch(entities, join_keys_types)
schema._export_to_c(entities_ptr_schema)
batch._export_to_c(entities_ptr_array)

batch, schema = map_to_record_batch(request_data)
schema._export_to_c(req_data_ptr_schema)
batch._export_to_c(req_data_ptr_array)
```

Under the hood, `allocate_schema_and_array` allocates a pointer (`struct ArrowSchema*` and `struct ArrowArray*`) in native memory (i.e. the C layer) using `cffi`.
Next, the RecordBatch exports to this pointer using [`_export_to_c`](https://github.com/apache/arrow/blob/master/python/pyarrow/table.pxi#L2509), which uses [`ExportRecordBatch`](https://arrow.apache.org/docs/cpp/api/c_abi.html#_CPPv417ExportRecordBatchRK11RecordBatchP10ArrowArrayP11ArrowSchema) under the hood.

As per the documentation for ExportRecordBatch:
> Status ExportRecordBatch(const RecordBatch &batch, struct ArrowArray *out, struct ArrowSchema *out_schema = NULLPTR)
> Export C++ RecordBatch using the C data interface format.
> 
> The record batch is exported as if it were a struct array. The resulting ArrowArray struct keeps the record batch data and buffers alive until its release callback is called by the consumer.

This is why `GetOnlineFeatures()` in `online_features.go` calls `record.Release()` as below:
```
entitiesRecord, err := readArrowRecord(entities)
if err != nil {
    return err
}
defer entitiesRecord.Release()
...
requestDataRecords, err := readArrowRecord(requestData)
if err != nil {
    return err
}
defer requestDataRecords.Release()
```

Additionally, we need to pass in a pair of pointers to `GetOnlineFeatures()` that are populated by the Go layer, and the resultant feature values can be passed back to Python (via the C layer) using zero-copy semantics.
That happens as follows:
```
(
    features_c_schema,
    features_ptr_schema,
    features_c_array,
    features_ptr_array,
) = allocate_schema_and_array()

...

record_batch = pa.RecordBatch._import_from_c(
    features_ptr_array, features_ptr_schema
)
```

The corresponding Go code that exports this data is:
```
result := array.NewRecord(arrow.NewSchema(outputFields, nil), outputColumns, int64(numRows))

cdata.ExportArrowRecordBatch(result,
    cdata.ArrayFromPtr(output.DataPtr),
    cdata.SchemaFromPtr(output.SchemaPtr))
```

The documentation for `ExportArrowRecordBatch` is great. It has this super useful caveat:

> // The release function on the populated CArrowArray will properly decrease the reference counts,
> // and release the memory if the record has already been released. But since this must be explicitly
> // done, make sure it is released so that you do not create a memory leak.

This implies that the reciever is on the hook for explicitly releasing this memory. 

However, we're using `_import_from_c`, which uses [`ImportRecordBatch`](https://arrow.apache.org/docs/cpp/api/c_abi.html#_CPPv417ImportRecordBatchP10ArrowArrayP11ArrowSchema), which implies that the receiver of the RecordBatch is the new owner of the data.
This is wrapped by pyarrow - and when the corresponding python object goes out of scope, it should clean up the underlying record batch.   

Another thing to note (which I'm not sure may be the source of issues) is that Arrow has the concept of [Memory Pools](https://arrow.apache.org/docs/python/api/memory.html#memory-pools).
Memory pools can be set in python as well as in Go. I *believe* that if we use the CGoArrowAllocator, that uses whatever pool C++ uses, which should be the same as the one used by PyArrow. But this should be vetted. 


### References
- https://arrow.apache.org/docs/format/CDataInterface.html#memory-management
- https://arrow.apache.org/docs/python/memory.html