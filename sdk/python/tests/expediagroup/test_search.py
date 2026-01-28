import unittest
from datetime import datetime

from feast.expediagroup.search import (
    ExpediaProjectAndRelatedFeatureViews,
    ExpediaSearchFeatureViewsRequest,
    ExpediaSearchFeatureViewsResponse,
    ExpediaSearchProjectsRequest,
    ExpediaSearchProjectsResponse,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.project import Project


class TestExpediaSearch(unittest.TestCase):
    def setUp(self):
        self.project = Project(name="test_project")
        self.source = FileSource(path="test_path")
        self.feature_view = FeatureView(name="test_feature_view", source=self.source)
        self.created_at = datetime(2024, 1, 1, 12, 0, 0)
        self.updated_at = datetime(2024, 1, 2, 13, 30, 0)

    def test_expedia_project_and_related_feature_views_init(self):
        obj = ExpediaProjectAndRelatedFeatureViews(
            project=self.project,
            feature_views=[self.feature_view],
        )
        self.assertEqual(obj.project, self.project)
        self.assertEqual(obj.feature_views, [self.feature_view])

    def test_expedia_project_and_related_feature_views_proto_roundtrip(self):
        obj = ExpediaProjectAndRelatedFeatureViews(
            project=self.project,
            feature_views=[self.feature_view],
        )
        proto = obj.to_proto()
        obj2 = ExpediaProjectAndRelatedFeatureViews.from_proto(proto)
        self.assertEqual(obj, obj2)

    def test_expedia_search_feature_views_request_init(self):
        req = ExpediaSearchFeatureViewsRequest(
            search_text="foo",
            online=True,
            application="app",
            team="team",
            created_at=self.created_at,
            updated_at=self.updated_at,
            page_size=5,
            page_index=2,
        )
        self.assertEqual(req.search_text, "foo")
        self.assertEqual(req.online, True)
        self.assertEqual(req.application, "app")
        self.assertEqual(req.team, "team")
        self.assertEqual(req.created_at, self.created_at)
        self.assertEqual(req.updated_at, self.updated_at)
        self.assertEqual(req.page_size, 5)
        self.assertEqual(req.page_index, 2)

    def test_expedia_search_feature_views_request_proto_roundtrip(self):
        req = ExpediaSearchFeatureViewsRequest(
            search_text="foo",
            online=False,
            application="app",
            team="team",
            created_at=self.created_at,
            updated_at=self.updated_at,
            page_size=7,
            page_index=3,
        )
        proto = req.to_proto()
        req2 = ExpediaSearchFeatureViewsRequest.from_proto(proto)
        self.assertEqual(req, req2)

    def test_expedia_search_feature_views_response_proto_roundtrip(self):
        resp = ExpediaSearchFeatureViewsResponse(
            feature_views=[self.feature_view],
            total_feature_views=1,
            total_page_indices=1,
        )
        proto = resp.to_proto()
        resp2 = ExpediaSearchFeatureViewsResponse.from_proto(proto)
        self.assertEqual(resp, resp2)

    def test_expedia_search_projects_request_init(self):
        req = ExpediaSearchProjectsRequest(
            search_text="bar",
            updated_at=self.updated_at,
            page_size=8,
            page_index=4,
        )
        self.assertEqual(req.search_text, "bar")
        self.assertEqual(req.updated_at, self.updated_at)
        self.assertEqual(req.page_size, 8)
        self.assertEqual(req.page_index, 4)

    def test_expedia_search_projects_request_proto_roundtrip(self):
        req = ExpediaSearchProjectsRequest(
            search_text="bar",
            updated_at=self.updated_at,
            page_size=8,
            page_index=4,
        )
        proto = req.to_proto()
        req2 = ExpediaSearchProjectsRequest.from_proto(proto)
        self.assertEqual(req, req2)

    def test_expedia_search_projects_response_proto_roundtrip(self):
        obj = ExpediaProjectAndRelatedFeatureViews(
            project=self.project,
            feature_views=[self.feature_view],
        )
        resp = ExpediaSearchProjectsResponse(
            projects_and_related_feature_views=[obj],
            total_projects=1,
            total_page_indices=1,
        )
        proto = resp.to_proto()
        resp2 = ExpediaSearchProjectsResponse.from_proto(proto)
        self.assertEqual(resp, resp2)


if __name__ == "__main__":
    unittest.main()
