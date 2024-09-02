import unittest
from datetime import datetime, timezone

from feast.project import Project
from feast.protos.feast.core.Project_pb2 import Project as ProjectProto
from feast.protos.feast.core.Project_pb2 import ProjectMeta as ProjectMetaProto
from feast.protos.feast.core.Project_pb2 import ProjectSpec as ProjectSpecProto


class TestProject(unittest.TestCase):
    def setUp(self):
        self.project_name = "test_project"
        self.description = "Test project description"
        self.tags = {"env": "test"}
        self.owner = "test_owner"
        self.created_timestamp = datetime.now(tz=timezone.utc)
        self.last_updated_timestamp = datetime.now(tz=timezone.utc)

    def test_initialization(self):
        project = Project(
            name=self.project_name,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            created_timestamp=self.created_timestamp,
            last_updated_timestamp=self.last_updated_timestamp,
        )
        self.assertEqual(project.name, self.project_name)
        self.assertEqual(project.description, self.description)
        self.assertEqual(project.tags, self.tags)
        self.assertEqual(project.owner, self.owner)
        self.assertEqual(project.created_timestamp, self.created_timestamp)
        self.assertEqual(project.last_updated_timestamp, self.last_updated_timestamp)

    def test_equality(self):
        project1 = Project(name=self.project_name)
        project2 = Project(name=self.project_name)
        project3 = Project(name="different_project")
        self.assertTrue(
            project1.name == project2.name
            and project1.description == project2.description
            and project1.tags == project2.tags
            and project1.owner == project2.owner
        )
        self.assertFalse(
            project1.name == project3.name
            and project1.description == project3.description
            and project1.tags == project3.tags
            and project1.owner == project3.owner
        )

    def test_is_valid(self):
        project = Project(name=self.project_name)
        project.is_valid()
        with self.assertRaises(ValueError):
            invalid_project = Project(name="")
            invalid_project.is_valid()

    def test_from_proto(self):
        meta = ProjectMetaProto()
        meta.created_timestamp.FromDatetime(self.created_timestamp)
        meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)
        project_proto = ProjectProto(
            spec=ProjectSpecProto(
                name=self.project_name,
                description=self.description,
                tags=self.tags,
                owner=self.owner,
            ),
            meta=meta,
        )
        project = Project.from_proto(project_proto)
        self.assertEqual(project.name, self.project_name)
        self.assertEqual(project.description, self.description)
        self.assertEqual(project.tags, self.tags)
        self.assertEqual(project.owner, self.owner)
        self.assertEqual(project.created_timestamp, self.created_timestamp)
        self.assertEqual(project.last_updated_timestamp, self.last_updated_timestamp)

    def test_to_proto(self):
        project = Project(
            name=self.project_name,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            created_timestamp=self.created_timestamp,
            last_updated_timestamp=self.last_updated_timestamp,
        )
        project_proto = project.to_proto()
        self.assertEqual(project_proto.spec.name, self.project_name)
        self.assertEqual(project_proto.spec.description, self.description)
        self.assertEqual(project_proto.spec.tags, self.tags)
        self.assertEqual(project_proto.spec.owner, self.owner)
        self.assertEqual(
            project_proto.meta.created_timestamp.ToDatetime().replace(
                tzinfo=timezone.utc
            ),
            self.created_timestamp,
        )
        self.assertEqual(
            project_proto.meta.last_updated_timestamp.ToDatetime().replace(
                tzinfo=timezone.utc
            ),
            self.last_updated_timestamp,
        )

    def test_to_proto_and_back(self):
        project = Project(
            name=self.project_name,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            created_timestamp=self.created_timestamp,
            last_updated_timestamp=self.last_updated_timestamp,
        )
        project_proto = project.to_proto()
        project_from_proto = Project.from_proto(project_proto)
        self.assertEqual(project, project_from_proto)


if __name__ == "__main__":
    unittest.main()
