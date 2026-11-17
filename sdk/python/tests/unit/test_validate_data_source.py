from typing import List, Optional, Tuple

from feast.protos.feast.registry import RegistryServer_pb2
from feast.registry_server import RegistryServer


class _FakeSource:
    def __init__(
        self,
        columns: Optional[List[Tuple[str, str]]] = None,
        probe_exc: Optional[Exception] = None,
        validate_exc: Optional[Exception] = None,
    ):
        self._columns = columns
        self._probe_exc = probe_exc
        self._validate_exc = validate_exc

    def get_table_column_names_and_types(self, config):
        if self._probe_exc is not None:
            raise self._probe_exc
        return self._columns if self._columns is not None else []

    def validate(self, config):
        if self._validate_exc is not None:
            raise self._validate_exc
        return None


class _FakeRegistry:
    def __init__(self, source):
        self._source = source

    def get_data_source(self, name, project, allow_cache=False):
        return self._source


class _FakeConfig:
    pass


class _FakeStore:
    def __init__(self):
        self.config = _FakeConfig()


def _validate(source) -> RegistryServer_pb2.ValidateDataSourceResponse:
    server = RegistryServer(_FakeRegistry(source), store=_FakeStore())
    request = RegistryServer_pb2.ValidateDataSourceRequest(
        name="ds", project="proj", allow_cache=True
    )
    return server.ValidateDataSource(request, None)


def test_non_empty_schema_probe_reports_success():
    resp = _validate(_FakeSource(columns=[("a", "INT64"), ("b", "STRING")]))
    assert resp.status == "success"
    assert len(resp.columns) == 2


def test_empty_schema_probe_is_not_reported_as_success():
    resp = _validate(_FakeSource(columns=[]))
    assert resp.status == "not_applicable"
    assert len(resp.columns) == 0


def test_unimplemented_probe_falls_back_to_validate_success():
    resp = _validate(_FakeSource(probe_exc=NotImplementedError()))
    assert resp.status == "success"


def test_unimplemented_probe_and_validate_is_not_applicable():
    resp = _validate(
        _FakeSource(probe_exc=NotImplementedError(), validate_exc=NotImplementedError())
    )
    assert resp.status == "not_applicable"


def test_probe_connection_error_reports_failed():
    resp = _validate(_FakeSource(probe_exc=FileNotFoundError("no such file")))
    assert resp.status == "failed"
    assert "no such file" in resp.message
