# Deprecated APIs and features

This document contains a summary of Feast APIs and features that have been deprecated or are scheduled to be deprecated, starting with version 0.19.

## Index

The following table lists deprecated APIs and features, organized by release. Each item is linked to a section later in this guide that describes the reason for deprecation.


| API or Feature                                                                       | Removed by   |
| :----------------------------------------------------------------------------------- | :----------- |
| [`table_ref` parameter in `BigQuerySource`](#table_ref-parameter-in-bigquerysource)  | 0.20         |
| [positional arguments in `Entity`](#positional-arguments-in-entity)                  | 0.23         |

## Deprecated features

### `table_ref` parameter in `BigQuerySource`

The `table_ref` parameter in `BigQuerySource` is being deprecated in favor of `table`.

### positional arguments in `Entity`

Positional arguments in `Entity` are being deprecated in favor of keyword arguments. 
