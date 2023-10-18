# Batch Materialization Engine

A batch materialization engine is a component of Feast that's responsible for moving data from the offline store into the online store.

A materialization engine abstracts over specific technologies or frameworks that are used to materialize data. It allows users to use a pure local serialized approach (which is the default LocalMaterializationEngine), or delegates the materialization to seperate components (e.g. AWS Lambda, as implemented by the the LambdaMaterializaionEngine).

If the built-in engines are not sufficient, you can create your own custom materialization engine. Please see [this guide](../../how-to-guides/customizing-feast/creating-a-custom-materialization-engine.md) for more details.

Please see [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md#overview) for configuring engines.
