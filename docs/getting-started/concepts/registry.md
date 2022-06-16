# Registry

The Feast registry is where all applied Feast objects (e.g. Feature views, entities, etc) are stored. The registry exposes methods to apply, list, retrieve and delete these objects. The registry is abstraction, with multiple possible implementations.

By default, the registry Feast uses a file-based registry implementation, which stores the protobuf representation of the registry as a serialized file. This registry file can be stored in a local file system, or in cloud storage (in, say, S3 or GCS).

However, there's inherent limitations with a file-based registry, since changing a single field in the registry requires re-writing the whole registry file. With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

Alternatively, a [SQL Registry](../../tutorials/using-scalable-registry.md) can be used for a more scalable registry. 