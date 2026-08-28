import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("publish_versioned_docs.py")
SPEC = importlib.util.spec_from_file_location("publish_versioned_docs", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
publisher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = publisher
SPEC.loader.exec_module(publisher)


class FakeApi:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, path, payload=None, expected=(200,)):
        self.calls.append((method, path, payload, expected))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def site_space(identifier, title, *, default=False, draft=False):
    return {
        "id": identifier,
        "title": title,
        "default": default,
        "draft": draft,
        "space": {"id": f"space-{identifier}", "title": title},
    }


def successful_import(branch):
    return {
        "url": f"https://github.com/feast-dev/feast/tree/{branch}",
        "operation": {"state": "success", "direction": "import"},
    }


class VersionInfoTests(unittest.TestCase):
    def test_parses_release_version(self):
        self.assertEqual(
            publisher.VersionInfo.parse("v0.66.0"),
            ("0.66.0", "v0.66.0", "v0.66-branch"),
        )

    def test_rejects_prerelease_version(self):
        with self.assertRaisesRegex(ValueError, "stable semantic version"):
            publisher.VersionInfo.parse("0.66.0-rc1")


class GitHubPublisherTests(unittest.TestCase):
    def test_creates_missing_release_branch_at_tag(self):
        api = FakeApi(
            [
                {"sha": "release-sha"},
                publisher.ApiError(404, "missing"),
                {"ref": "refs/heads/v0.66-branch"},
            ]
        )
        github = publisher.GitHubPublisher(api, "feast-dev/feast")

        self.assertEqual(
            github.ensure_release_branch("v0.66.0", "v0.66-branch"),
            "release-sha",
        )
        self.assertEqual(
            api.calls[-1],
            (
                "POST",
                "/repos/feast-dev/feast/git/refs",
                {"ref": "refs/heads/v0.66-branch", "sha": "release-sha"},
                (201,),
            ),
        )

    def test_reuses_matching_release_branch(self):
        api = FakeApi([{"sha": "release-sha"}, {"commit": {"sha": "release-sha"}}])
        github = publisher.GitHubPublisher(api, "feast-dev/feast")

        github.ensure_release_branch("v0.66.0", "v0.66-branch")

        self.assertEqual(len(api.calls), 2)

    def test_refuses_to_move_conflicting_release_branch(self):
        api = FakeApi([{"sha": "release-sha"}, {"commit": {"sha": "different-sha"}}])
        github = publisher.GitHubPublisher(api, "feast-dev/feast")

        with self.assertRaisesRegex(RuntimeError, "refusing to move"):
            github.ensure_release_branch("v0.66.0", "v0.66-branch")

        self.assertEqual(len(api.calls), 2)


class GitBookPublisherTests(unittest.TestCase):
    def _publisher(self, responses):
        api = FakeApi(responses)
        gitbook = publisher.GitBookPublisher(
            api,
            "org",
            "site",
            "feast-dev/feast",
            sync_timeout=0,
            sync_poll_interval=0,
        )
        return gitbook, api

    def test_creates_imports_and_publishes_new_version(self):
        old_default = site_space("old", "v0.64-branch", default=True)
        new_space = site_space("new", "v0.64-branch", draft=True)
        new_default = site_space("new", "v0.66-branch", default=True)
        gitbook, api = self._publisher(
            [
                {"items": [old_default]},
                {"items": [old_default]},
                new_space,
                {},
                None,
                successful_import("v0.66-branch"),
                {},
                {},
                {},
                {"items": [new_default]},
            ]
        )

        gitbook.publish_version("v0.66-branch")

        self.assertIn(
            (
                "POST",
                "/orgs/org/sites/site/site-spaces/old/duplicate",
                {"draft": True},
                (201,),
            ),
            api.calls,
        )
        self.assertIn(
            (
                "PATCH",
                "/orgs/org/sites/site",
                {"defaultSiteSpace": "new"},
                (200,),
            ),
            api.calls,
        )

    def test_reuses_existing_version_space(self):
        target = site_space("new", "v0.66-branch")
        new_default = site_space("new", "v0.66-branch", default=True)
        gitbook, api = self._publisher(
            [
                {"items": [target]},
                None,
                successful_import("v0.66-branch"),
                {},
                {},
                {},
                {"items": [new_default]},
            ]
        )

        gitbook.publish_version("v0.66-branch")

        self.assertFalse(any(call[1].endswith("/duplicate") for call in api.calls))

    def test_failed_import_does_not_change_public_default(self):
        target = site_space("new", "v0.66-branch", draft=True)
        gitbook, api = self._publisher(
            [
                {"items": [target]},
                None,
                {
                    "url": "https://github.com/feast-dev/feast/tree/v0.66-branch",
                    "operation": {
                        "state": "failure",
                        "direction": "import",
                        "error": "invalid content",
                    },
                },
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "invalid content"):
            gitbook.publish_version("v0.66-branch")

        self.assertFalse(any(call[0] == "PATCH" for call in api.calls))

    def test_fails_when_default_space_readback_does_not_match(self):
        target = site_space("new", "v0.66-branch")
        old_default = site_space("old", "v0.64-branch", default=True)
        gitbook, _api = self._publisher(
            [
                {"items": [target]},
                None,
                successful_import("v0.66-branch"),
                {},
                {},
                {},
                {"items": [old_default]},
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "did not publish"):
            gitbook.publish_version("v0.66-branch")

    def test_waits_for_default_space_readback_to_match(self):
        target = site_space("new", "v0.66-branch")
        old_default = site_space("old", "v0.64-branch", default=True)
        new_default = site_space("new", "v0.66-branch", default=True)
        api = FakeApi(
            [
                {"items": [target]},
                None,
                successful_import("v0.66-branch"),
                {},
                {},
                {},
                {"items": [old_default]},
                {"items": [new_default]},
            ]
        )
        gitbook = publisher.GitBookPublisher(
            api,
            "org",
            "site",
            "feast-dev/feast",
            sync_timeout=1,
            sync_poll_interval=0,
        )

        gitbook.publish_version("v0.66-branch")

        default_reads = [
            call for call in api.calls if call[0] == "GET" and "default=true" in call[1]
        ]
        self.assertEqual(len(default_reads), 2)


if __name__ == "__main__":
    unittest.main()
