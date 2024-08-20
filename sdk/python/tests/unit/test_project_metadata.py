import unittest
from datetime import datetime, timezone

from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.Registry_pb2 import ProjectMetadata as ProjectMetadataProto


class TestProjectMetadata(unittest.TestCase):
    def setUp(self):
        self.project_name = "test_project"
        self.project_uuid = "123e4567-e89b-12d3-a456-426614174000"
        self.timestamp = datetime(2021, 1, 1, tzinfo=timezone.utc)

    def test_initialization(self):
        metadata = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.timestamp,
        )
        self.assertEqual(metadata.project_name, self.project_name)
        self.assertEqual(metadata.project_uuid, self.project_uuid)
        self.assertEqual(metadata.last_updated_timestamp, self.timestamp)

    def test_initialization_with_default_last_updated_timestamp(self):
        metadata = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
        )
        self.assertEqual(metadata.project_name, self.project_name)
        self.assertEqual(metadata.project_uuid, self.project_uuid)
        self.assertEqual(
            metadata.last_updated_timestamp, datetime.fromtimestamp(1, tz=timezone.utc)
        )

    def test_initialization_without_project_name(self):
        with self.assertRaises(ValueError):
            ProjectMetadata()

    def test_equality(self):
        metadata1 = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.timestamp,
        )
        metadata2 = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.timestamp,
        )
        self.assertEqual(metadata1, metadata2)

    def test_hash(self):
        metadata = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.timestamp,
        )
        self.assertEqual(
            hash(metadata), hash((self.project_name, self.project_uuid, self.timestamp))
        )

    def test_from_proto(self):
        proto = ProjectMetadataProto(
            project=self.project_name,
            project_uuid=self.project_uuid,
        )
        proto.last_updated_timestamp.FromDatetime(self.timestamp)
        metadata = ProjectMetadata.from_proto(proto)
        self.assertEqual(metadata.project_name, self.project_name)
        self.assertEqual(metadata.project_uuid, self.project_uuid)
        self.assertEqual(metadata.last_updated_timestamp, self.timestamp)

    def test_to_proto(self):
        metadata = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.timestamp,
        )
        proto = metadata.to_proto()
        self.assertEqual(proto.project, self.project_name)
        self.assertEqual(proto.project_uuid, self.project_uuid)
        self.assertEqual(
            proto.last_updated_timestamp.ToDatetime().replace(tzinfo=timezone.utc),
            self.timestamp,
        )

    def test_conversion_to_proto_and_back(self):
        metadata = ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.timestamp,
        )
        proto = metadata.to_proto()
        metadata_from_proto = ProjectMetadata.from_proto(proto)
        self.assertEqual(metadata, metadata_from_proto)


if __name__ == "__main__":
    unittest.main()
