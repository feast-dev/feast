import pytest
from feast.sdk.utils.gs_utils import is_gs_path

def test_is_gs_path():
    assert is_gs_path("gs://valid/gs/file.csv") == True
    assert is_gs_path("local/path/file.csv") == False