from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow
import pytest

from feast.infra.compute_engines.backends.factory import BackendFactory
from feast.infra.compute_engines.backends.pandas_backend import PandasBackend


class TestBackendFactoryFromName:
    def test_pandas_backend(self):
        backend = BackendFactory.from_name("pandas")
        assert isinstance(backend, PandasBackend)

    @patch(
        "feast.infra.compute_engines.backends.factory.BackendFactory._get_polars_backend"
    )
    def test_polars_backend(self, mock_get_polars):
        mock_backend = MagicMock()
        mock_get_polars.return_value = mock_backend

        result = BackendFactory.from_name("polars")
        assert result is mock_backend

    def test_unsupported_name_raises(self):
        with pytest.raises(ValueError, match="Unsupported backend name"):
            BackendFactory.from_name("dask")


class TestBackendFactoryInferFromEntityDf:
    def test_pandas_dataframe_returns_pandas_backend(self):
        """A non-empty pandas DataFrame is detected via isinstance check."""
        df = pd.DataFrame({"a": [1, 2]})
        backend = BackendFactory.infer_from_entity_df(df)
        assert isinstance(backend, PandasBackend)

    def test_empty_pandas_dataframe_returns_pandas_backend(self):
        """An empty pandas DataFrame returns PandasBackend."""
        df = pd.DataFrame()
        backend = BackendFactory.infer_from_entity_df(df)
        assert isinstance(backend, PandasBackend)

    def test_pyarrow_table(self):
        table = pyarrow.table({"a": [1, 2]})
        backend = BackendFactory.infer_from_entity_df(table)
        assert isinstance(backend, PandasBackend)

    def test_none_input(self):
        backend = BackendFactory.infer_from_entity_df(None)
        assert isinstance(backend, PandasBackend)

    def test_empty_string_input(self):
        backend = BackendFactory.infer_from_entity_df("")
        assert isinstance(backend, PandasBackend)
