from feast.importer import import_class

TRANSFORMATION_CLASS_FOR_TYPE = {
    "python": "feast.transformation.python_transformation.PythonTransformation",
    "pandas": "feast.transformation.pandas_transformation.PandasTransformation",
    "substrait": "feast.transformation.substrait_transformation.SubstraitTransformation",
    "sql": "feast.transformation.sql_transformation.SQLTransformation",
    "spark_sql": "feast.transformation.spark_transformation.SparkTransformation",
    "spark": "feast.transformation.spark_transformation.SparkTransformation",
    "ray": "feast.transformation.ray_transformation.RayTransformation",
}


def get_transformation_class_from_type(transformation_type: str):
    if transformation_type in TRANSFORMATION_CLASS_FOR_TYPE:
        transformation_type = TRANSFORMATION_CLASS_FOR_TYPE[transformation_type]
    elif not transformation_type.endswith("Transformation"):
        raise ValueError(
            f"Invalid transformation type: {transformation_type}. Choose from {list(TRANSFORMATION_CLASS_FOR_TYPE.keys())}."
        )
    module_name, transformation_class_type = transformation_type.rsplit(".", 1)
    return import_class(
        module_name, transformation_class_type, transformation_class_type
    )
