enum FEAST_FCO_TYPES {
  dataSource = "dataSource",
  entity = "entity",
  featureView = "featureView",
  featureService = "featureService",
}

enum FEAST_FEATURE_VALUE_TYPES {
  FLOAT = "FLOAT",
  INT64 = "INT64",
  STRING = "STRING",
  BOOL = "BOOL",
  BYTES = "BYTES",
  INT32 = "INT32",
  DOUBLE = "DOUBLE",
  UNIX_TIMESTAMP = "UNIX_TIMESTAMP"
}

export { FEAST_FCO_TYPES, FEAST_FEATURE_VALUE_TYPES };
