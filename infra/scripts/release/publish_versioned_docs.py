"""Publish a released Feast minor version as the default GitBook documentation.

The release workflow supplies the released semantic version and credentials through
environment variables. The operation is idempotent: matching branches and GitBook
spaces are reused, while conflicting release branches fail without being moved.
"""

import argparse
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, NamedTuple


GITHUB_API_URL = "https://api.github.com"
GITBOOK_API_URL = "https://api.gitbook.com/v1"
GITHUB_API_VERSION = "2022-11-28"


class ApiError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class VersionInfo(NamedTuple):
    version: str
    tag: str
    branch: str

    @classmethod
    def parse(cls, value: str) -> "VersionInfo":
        match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", value.strip())
        if match is None:
            raise ValueError(f"expected a stable semantic version, got {value!r}")
        major, minor, patch = match.groups()
        version = f"{major}.{minor}.{patch}"
        return cls(version, f"v{version}", f"v{major}.{minor}-branch")


class JsonApi:
    def __init__(
        self,
        base_url: str,
        token: str,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.extra_headers = extra_headers or {}

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        expected: tuple[int, ...] = (200,),
    ) -> Any:
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "feast-release-docs",
            **self.extra_headers,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = response.status
                response_body = response.read()
        except urllib.error.HTTPError as error:
            status = error.code
            response_body = error.read()
        except urllib.error.URLError as error:
            raise RuntimeError(
                f"request to {self.base_url} failed: {error.reason}"
            ) from error

        if status not in expected:
            detail = response_body.decode(errors="replace").strip()
            raise ApiError(status, f"{method} {path} returned HTTP {status}: {detail}")
        if not response_body:
            return None
        return json.loads(response_body)


class GitHubPublisher:
    def __init__(self, api: JsonApi, repository: str) -> None:
        self.api = api
        self.repository = repository
        self.repository_path = urllib.parse.quote(repository, safe="/")

    def _commit_sha(self, tag: str) -> str:
        encoded_tag = urllib.parse.quote(tag, safe="")
        commit = self.api.request(
            "GET", f"/repos/{self.repository_path}/commits/{encoded_tag}"
        )
        return commit["sha"]

    def _branch_sha(self, branch: str) -> str | None:
        encoded_branch = urllib.parse.quote(branch, safe="")
        try:
            result = self.api.request(
                "GET", f"/repos/{self.repository_path}/branches/{encoded_branch}"
            )
        except ApiError as error:
            if error.status == 404:
                return None
            raise
        return result["commit"]["sha"]

    def ensure_release_branch(self, tag: str, branch: str) -> str:
        tag_sha = self._commit_sha(tag)
        branch_sha = self._branch_sha(branch)
        if branch_sha is None:
            self.api.request(
                "POST",
                f"/repos/{self.repository_path}/git/refs",
                {"ref": f"refs/heads/{branch}", "sha": tag_sha},
                expected=(201,),
            )
            print(f"Created {branch} at {tag_sha}")
            return tag_sha
        if branch_sha != tag_sha:
            raise RuntimeError(
                f"refusing to move existing {branch}: {branch_sha} != released {tag_sha}"
            )
        print(f"Verified existing {branch} at {tag_sha}")
        return tag_sha


class GitBookPublisher:
    def __init__(
        self,
        api: JsonApi,
        organization_id: str,
        site_id: str,
        repository: str,
        sync_timeout: int = 180,
        sync_poll_interval: int = 5,
    ) -> None:
        self.api = api
        self.organization_id = urllib.parse.quote(organization_id, safe="")
        self.site_id = urllib.parse.quote(site_id, safe="")
        self.repository = repository
        self.sync_timeout = sync_timeout
        self.sync_poll_interval = sync_poll_interval

    @property
    def _site_path(self) -> str:
        return f"/orgs/{self.organization_id}/sites/{self.site_id}"

    def _list_site_spaces(self, default: bool | None = None) -> list[dict[str, Any]]:
        query = {"limit": "1000"}
        if default is not None:
            query["default"] = str(default).lower()
        result = self.api.request(
            "GET", f"{self._site_path}/site-spaces?{urllib.parse.urlencode(query)}"
        )
        return result["items"]

    @staticmethod
    def _space_title(site_space: dict[str, Any]) -> str | None:
        return site_space.get("space", {}).get("title") or site_space.get("title")

    def _find_site_space(self, title: str) -> dict[str, Any] | None:
        return next(
            (
                site_space
                for site_space in self._list_site_spaces()
                if self._space_title(site_space) == title
            ),
            None,
        )

    def _duplicate_default_space(self, title: str) -> dict[str, Any]:
        defaults = self._list_site_spaces(default=True)
        if len(defaults) != 1:
            raise RuntimeError(
                f"expected one default GitBook site space, found {len(defaults)}"
            )
        source_id = urllib.parse.quote(defaults[0]["id"], safe="")
        site_space = self.api.request(
            "POST",
            f"{self._site_path}/site-spaces/{source_id}/duplicate",
            {"draft": True},
            expected=(201,),
        )
        space_id = urllib.parse.quote(site_space["space"]["id"], safe="")
        self.api.request("PATCH", f"/spaces/{space_id}", {"title": title})
        site_space["space"]["title"] = title
        return site_space

    def _import_branch(self, space_id: str, branch: str) -> None:
        encoded_space_id = urllib.parse.quote(space_id, safe="")
        self.api.request(
            "POST",
            f"/spaces/{encoded_space_id}/git/import",
            {
                "url": f"https://github.com/{self.repository}",
                "ref": f"refs/heads/{branch}",
                "force": True,
            },
            expected=(204,),
        )

    def _wait_for_import(self, space_id: str, branch: str) -> None:
        encoded_space_id = urllib.parse.quote(space_id, safe="")
        expected_suffix = f"/tree/{branch}"
        deadline = time.monotonic() + self.sync_timeout
        while True:
            info = self.api.request("GET", f"/spaces/{encoded_space_id}/git/info")
            operation = info.get("operation") or {}
            state = operation.get("state")
            branch_matches = info.get("url", "").rstrip("/").endswith(expected_suffix)
            if branch_matches and state == "success":
                return
            if branch_matches and state in {"failure", "timeout"}:
                detail = operation.get("error") or state
                raise RuntimeError(f"GitBook import for {branch} failed: {detail}")
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"timed out waiting for GitBook to import {branch} after "
                    f"{self.sync_timeout}s"
                )
            time.sleep(self.sync_poll_interval)

    def _wait_for_default(self, site_space_id: str, branch: str) -> None:
        deadline = time.monotonic() + self.sync_timeout
        while True:
            defaults = self._list_site_spaces(default=True)
            if (
                len(defaults) == 1
                and defaults[0]["id"] == site_space_id
                and self._space_title(defaults[0]) == branch
                and not defaults[0].get("draft", False)
            ):
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"GitBook did not publish {branch} as the default space after "
                    f"{self.sync_timeout}s"
                )
            time.sleep(self.sync_poll_interval)

    def publish_version(self, branch: str) -> None:
        site_space = self._find_site_space(branch)
        if site_space is None:
            site_space = self._duplicate_default_space(branch)
            print(f"Created draft GitBook space for {branch}")
        else:
            print(f"Reusing GitBook space for {branch}")

        site_space_id = site_space["id"]
        space_id = site_space["space"]["id"]
        self._import_branch(space_id, branch)
        self._wait_for_import(space_id, branch)

        encoded_site_space_id = urllib.parse.quote(site_space_id, safe="")
        self.api.request(
            "PATCH",
            f"{self._site_path}/site-spaces/{encoded_site_space_id}",
            {"path": branch, "draft": False},
        )
        self.api.request("PATCH", self._site_path, {"defaultSiteSpace": site_space_id})
        self.api.request("POST", f"{self._site_path}/publish")
        self._wait_for_default(site_space_id, branch)
        print(f"Published {branch} as the default GitBook documentation")


def publish_release(
    version: VersionInfo,
    github: GitHubPublisher,
    gitbook: GitBookPublisher,
) -> None:
    github.ensure_release_branch(version.tag, version.branch)
    gitbook.publish_version(version.branch)


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"error: {name} is required")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version", required=True, help="released version, such as 0.66.0"
    )
    parser.add_argument(
        "--repository",
        default=os.environ.get("GITHUB_REPOSITORY", "feast-dev/feast"),
        help="GitHub owner/repository",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sync-timeout", type=int, default=180)
    args = parser.parse_args()

    try:
        version = VersionInfo.parse(args.version)
    except ValueError as error:
        raise SystemExit(f"error: {error}") from error

    if args.dry_run:
        print(
            f"Would publish {version.tag} from {version.branch} as the default "
            "GitBook documentation"
        )
        return

    github_api = JsonApi(
        GITHUB_API_URL,
        _required_env("GITHUB_TOKEN"),
        {
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "Accept": "application/vnd.github+json",
        },
    )
    gitbook_api = JsonApi(GITBOOK_API_URL, _required_env("GITBOOK_TOKEN"))
    github = GitHubPublisher(github_api, args.repository)
    gitbook = GitBookPublisher(
        gitbook_api,
        _required_env("GITBOOK_ORG_ID"),
        _required_env("GITBOOK_SITE_ID"),
        args.repository,
        sync_timeout=args.sync_timeout,
    )
    publish_release(version, github, gitbook)


if __name__ == "__main__":
    main()
