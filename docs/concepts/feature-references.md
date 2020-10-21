# Feature References

## Overview

In Feast, each feature can be uniquely addressed through a feature reference. A feature reference is composed of the following components:

* Feature Table name
* Feature name

## Structure of a Feature Reference

A string based feature reference takes on the following format:

`<feature-table-name>:<feature-name>`

```python
# Feature references
feature_refs = [
   "driver_trips:average_daily_rides",
   "driver_trips:maximum_daily_rides",
   "driver_trips:rating",
]
```

Feature references only apply to a single `project`. Features cannot be retrieved across projects in a single request.

## Working with a Feature Reference

#### Feature Retrieval

Feature retrieval \(or serving\) is the process of retrieving either historical features or online features from Feast, for the purposes of training or serving a model.

Feast attempts to unify the process of retrieving features in both the historical and online case. It does this through the creation of feature references. One of the major advantages of using Feast is that you have a single semantic reference to a feature. These feature references can then be stored alongside your model and loaded into a serving layer where it can be used for online feature retrieval.

More information about how to perform feature retrieval for historical and online features can be found in the sections under **User Guide**.

