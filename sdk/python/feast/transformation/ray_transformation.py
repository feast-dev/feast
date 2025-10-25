import inspect
from typing import Any, Callable, Optional, cast, get_type_hints

import dill

from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode


class RayTransformation(Transformation):
    """
    Ray transformation for distributed data processing using Ray Datasets.

    Use this for computationally intensive transformations that benefit from
    parallel processing, such as:
    - Embedding generation (e.g., for RAG applications)
    - Image/video processing
    - Complex feature engineering
    - Large-scale data transformations

    Your UDF should accept a Ray Dataset and return a Ray Dataset, enabling
    native Ray operations and distributed processing across workers.

    Example - Basic transformation:
        >>> import ray.data
        >>> def my_ray_udf(ds: ray.data.Dataset) -> ray.data.Dataset:
        ...     return ds.map_batches(
        ...         lambda batch: batch,
        ...         batch_format="pandas"
        ...     )
        >>>
        >>> from feast.transformation.ray_transformation import RayTransformation
        >>> transform = RayTransformation(
        ...     udf=my_ray_udf,
        ...     udf_string="def my_ray_udf(ds): return ds.map_batches(...)"
        ... )

    Example - Embedding generation with stateful processing:
        >>> class EmbeddingProcessor:
        ...     def __init__(self):
        ...         # Model loaded once per worker (efficient!)
        ...         from sentence_transformers import SentenceTransformer
        ...         self.model = SentenceTransformer("all-MiniLM-L6-v2")
        ...
        ...     def __call__(self, batch):
        ...         embeddings = self.model.encode(batch["text"].tolist())
        ...         batch["embedding"] = embeddings.tolist()
        ...         return batch
        >>>
        >>> def generate_embeddings(ds: ray.data.Dataset) -> ray.data.Dataset:
        ...     return ds.map_batches(
        ...         EmbeddingProcessor,
        ...         batch_format="pandas",
        ...         concurrency=8  # Use 8 parallel workers
        ...     )

    Args:
        udf: Function that takes a Ray Dataset and returns a Ray Dataset
        udf_string: String representation of the UDF (for serialization)
        name: Optional name for the transformation
        tags: Optional metadata tags
        description: Optional description
        owner: Optional owner identifier

    Note:
        For best performance, use stateful classes with `map_batches` to avoid
        reloading models/resources for each batch. See the embedding example above.
    """

    def __new__(
        cls,
        udf: Optional[Callable[[Any], Any]] = None,
        udf_string: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ) -> "RayTransformation":
        # Handle Ray deserialization where parameters may not be provided
        if udf is None and udf_string is None:
            # Create a bare instance for deserialization
            instance = object.__new__(cls)
            return cast("RayTransformation", instance)

        # Ensure required parameters are not None before calling parent constructor
        if udf is None:
            raise ValueError("udf parameter cannot be None")
        if udf_string is None:
            raise ValueError("udf_string parameter cannot be None")

        return cast(
            "RayTransformation",
            super(RayTransformation, cls).__new__(
                cls,
                mode=TransformationMode.RAY,
                udf=udf,
                name=name,
                udf_string=udf_string,
                tags=tags,
                description=description,
                owner=owner,
            ),
        )

    def __init__(
        self,
        udf: Optional[Callable[[Any], Any]] = None,
        udf_string: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ):
        if udf is None and udf_string is None:
            return
        if udf is None:
            raise ValueError("udf parameter cannot be None")
        if udf_string is None:
            raise ValueError("udf_string parameter cannot be None")

        type_hints = get_type_hints(udf)
        return_annotation = type_hints.get("return", inspect._empty)

        if return_annotation not in (inspect._empty,):
            return_type_str = str(return_annotation)
            if "ray.data" not in return_type_str and "Dataset" not in return_type_str:
                import warnings

                warnings.warn(
                    f"Return type for RayTransformation should be ray.data.Dataset, got {return_annotation}. "
                    f"This may cause issues during execution."
                )

        super().__init__(
            mode=TransformationMode.RAY,
            udf=udf,
            name=name,
            udf_string=udf_string,
            tags=tags,
            description=description,
            owner=owner,
        )

    def transform(self, inputs: Any) -> Any:
        """
        Apply the transformation to a Ray Dataset.

        This method is called automatically by Feast during materialization.
        It applies your user-defined function (UDF) to the input data.

        Args:
            inputs: Ray Dataset containing the input data to transform.
                   The dataset will have columns matching your source schema.

        Returns:
            Transformed Ray Dataset with output features. The output schema
            will be automatically inferred or should match your explicitly
            defined schema in BatchFeatureView.

        Example:
            You typically don't call this directly - Feast calls it during:
            >>> # store.materialize_incremental(end_date)

            But you can test it manually:
            >>> import ray.data
            >>> import pandas as pd
            >>> test_data = ray.data.from_pandas(pd.DataFrame([{"text": "hello"}]))
            >>> # my_transformation = RayTransformation(udf=my_udf, udf_string="...")
            >>> # result = my_transformation.transform(test_data)
            >>> # result.show()
        """
        return self.udf(inputs)

    def infer_features(
        self,
        random_input: dict[str, list[Any]],
        *args,
        **kwargs,
    ) -> list[Field]:
        """
        Infer features from the Ray transformation.

        This method automatically infers the output schema by:
        1. Creating a Ray Dataset from sample input data
        2. Applying your transformation UDF
        3. Extracting the schema from the transformed output

        Args:
            random_input: Dictionary mapping column names to sample values.
                         Should contain representative data for your transformation.

        Returns:
            List of Field objects representing the inferred schema.

        Raises:
            TypeError: If schema inference fails. In this case, explicitly define
                      the schema in your BatchFeatureView using the `schema` parameter.

        Example:
            If your UDF adds an 'embedding' column, this will automatically
            detect it and infer its type from the output data.
        """
        try:
            import pandas as pd
            import ray.data

            # Create a Ray Dataset from the sample input
            df = pd.DataFrame.from_dict(random_input)
            ds = ray.data.from_pandas([df])

            # Apply the user's transformation
            output_ds = self.transform(ds)

            # Convert result to pandas to extract schema
            output_df = output_ds.to_pandas()

            # Infer field types from the output
            fields = []
            for feature_name, feature_type in zip(output_df.columns, output_df.dtypes):
                feature_value = output_df[feature_name].tolist()
                if len(feature_value) <= 0:
                    raise TypeError(
                        f"Cannot infer type for feature '{feature_name}': "
                        f"UDF returned empty output. Ensure your transformation "
                        f"returns at least one row of data."
                    )
                from feast.type_map import python_type_to_feast_value_type

                fields.append(
                    Field(
                        name=feature_name,
                        dtype=from_value_type(
                            python_type_to_feast_value_type(
                                feature_name,
                                value=feature_value[0],
                                type_name=str(feature_type),
                            )
                        ),
                    )
                )
            return fields
        except ImportError as e:
            raise TypeError(
                f"Failed to import required dependencies for RayTransformation: {e}. "
                f"Install Ray with: pip install feast[ray]"
            )
        except Exception as e:
            error_msg = (
                f"Failed to infer features from RayTransformation: {e}\n\n"
                f"💡 To fix this:\n"
                f"1. Explicitly define the schema in your BatchFeatureView:\n"
                f"   BatchFeatureView(\n"
                f"       schema=[\n"
                f"           Field(name='your_field', dtype=String),\n"
                f"           Field(name='embedding', dtype=Array(Float32)),\n"
                f"       ],\n"
                f"       ...\n"
                f"   )\n\n"
                f"2. Or ensure your UDF returns valid data when called with sample input:\n"
                f"   - Input sample: {list(random_input.keys())}\n"
                f"   - Check that your UDF can process this structure\n"
            )
            raise TypeError(error_msg)

    def __eq__(self, other):
        if not isinstance(other, RayTransformation):
            raise TypeError(
                "Comparisons should only involve RayTransformation class objects."
            )

        if (
            self.udf_string != other.udf_string
            or self.udf.__code__.co_code != other.udf.__code__.co_code
        ):
            return False

        return True

    @classmethod
    def from_proto(cls, user_defined_function_proto: UserDefinedFunctionProto):
        return RayTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
            name=user_defined_function_proto.name
            if user_defined_function_proto.name
            else None,
        )
