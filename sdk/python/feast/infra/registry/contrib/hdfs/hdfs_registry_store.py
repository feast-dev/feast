import uuid
import json
from pathlib import PurePosixPath, Path
from tempfile import TemporaryFile
from typing import Optional
from urllib.parse import urlparse

from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.utils import _utc_now


class HDFSRegistryStore(RegistryStore):
    """HDFS implementation of RegistryStore.
    registryConfig.path should be a hdfs path like hdfs://namenode:8020/path/to/registry.db
    """
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        try:
            from pyarrow.fs import HadoopFileSystem
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError
            raise FeastExtrasDependencyImportError("pyarrow.fs.HadoopFileSystem", str(e))
        uri = registry_config.path
        self._uri = urlparse(uri)
        if self._uri.scheme != "hdfs":
            raise ValueError(f"Unsupported scheme {self._uri.scheme} in HDFS path {uri}")
        self._hdfs = HadoopFileSystem(self._uri.hostname, self._uri.port or 8020)
        self._path = PurePosixPath(self._uri.path)

    def get_registry_proto(self):
        registry_proto = RegistryProto()
        if self._hdfs.exists(str(self._path)):
            with self._hdfs.open(str(self._path), "rb") as f:
                registry_proto.ParseFromString(f.read())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._uri.geturl()}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        try:
            self._hdfs.delete(str(self._path))
        except FileNotFoundError:
            # If the file deletion fails with FileNotFoundError, the file has already
            # been deleted.
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(_utc_now())
        dir_path = self._path.parent
        if not self._hdfs.exists(str(dir_path)):
            self._hdfs.mkdir(str(dir_path))
        with self._hdfs.open(str(self._path), "wb") as f:
            f.write(registry_proto.SerializeToString())

    def _write_registry(self, registry_proto: RegistryProto):
        """Write registry protobuf to HDFS."""
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(_utc_now())

        dir_path = self._path.parent
        if not self._hdfs.exists(str(dir_path)):
            self._hdfs.mkdir(str(dir_path))

        with self._hdfs.open(str(self._path), "wb") as f:
            f.write(registry_proto.SerializeToString())

    def set_project_metadata(self, project: str, key: str, value: str):
        """Set a custom project metadata key-value pair in the registry (HDFS backend)."""
        registry_proto = self.get_registry_proto()
        found = False

        for pm in registry_proto.project_metadata:
            if pm.project == project:
                # Load JSON metadata from project_uuid
                try:
                    meta = json.loads(pm.project_uuid) if pm.project_uuid else {}
                except Exception:
                    meta = {}

                if not isinstance(meta, dict):
                    meta = {}

                meta[key] = value
                pm.project_uuid = json.dumps(meta)
                found = True
                break

        if not found:
            # Create new ProjectMetadata entry
            from feast.project_metadata import ProjectMetadata
            pm = ProjectMetadata(project_name=project)
            pm.project_uuid = json.dumps({key: value})
            registry_proto.project_metadata.append(pm.to_proto())

        # Write back
        self.update_registry_proto(registry_proto)

    def get_project_metadata(self, project: str, key: str) -> Optional[str]:
        """Get custom project metadata key from registry (HDFS backend)."""
        registry_proto = self.get_registry_proto()

        for pm in registry_proto.project_metadata:
            if pm.project == project:
                try:
                    meta = json.loads(pm.project_uuid) if pm.project_uuid else {}
                except Exception:
                    meta = {}

                if not isinstance(meta, dict):
                    return None
                return meta.get(key, None)
        return None
