# Feast Minimal Ride-Hailing Example

This is a minimal example of using Feast. In this example, we will walk through the following:

1. Create and register an Entity, and Feature Table.
2. Retrieve and inspect registered Feature Tables.
3. Generate mock data and ingest into Feature Table's GCS batch source.
4. Perform historical retrieval with data from Feature Table's GCS batch source.
5. Populate online storage with data from Feature Table's GCS batch source.
6. Populate online storage with data from Feature Table's Kafka stream source.
7. Perform online retrieval using Feature References.
