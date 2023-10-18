// Release script for semantic-release.gitbook.io

const execSync = require("child_process").execSync;

// Get the current branch
const current_branch = execSync("git rev-parse --abbrev-ref HEAD").toString("utf8").trim();

// Validate the current branch
if (current_branch !== 'master') {
    // Should be a release branch like v0.18-branch
    is_valid = /v[0-9]\.[0-9][0-9]\-branch/gm.test(current_branch)
    if (!is_valid) {
        throw new Error(`Invalid branch name: ${current_branch}. Must be in release branch form like v0.18-branch or master`)
    }
}

// We have to dynamically generate all the supported branches for Feast because we use the `vA.B-branch` pattern for
// maintenance branches
possible_branches = [{name: "master"}, {name: current_branch}]

// Below is the configuration for semantic release
module.exports = {
    branches: possible_branches,
    plugins: [
        // Try to guess the type of release we should be doing (minor, patch)
        ["@semantic-release/commit-analyzer", {
            // Ensure that breaking changes trigger minor instead of major releases
            "releaseRules": [
                {breaking: true, release: 'minor'},
                {tag: 'Breaking', release: 'minor'},
                {type: '*!', release: 'minor'},
            ],
            // Ensure that the "BREAKING CHANGE" notes in commit footers are parsed
            "parserOpts": {
                "noteKeywords": ["BREAKING CHANGE", "BREAKING CHANGES"]
            }
        }],

        ["@semantic-release/exec", {
            // Validate the type of release we are doing
            "verifyReleaseCmd": "./infra/scripts/validate-release.sh  ${nextRelease.type} " + current_branch,

            // Bump all version files and build UI / update yarn.lock / helm charts
            "prepareCmd": "python ./infra/scripts/release/bump_file_versions.py ${lastRelease.version} ${nextRelease.version}; make build-ui; make build-helm-docs"
        }],

        ["@semantic-release/release-notes-generator", {
            // Ensure that a "Breaking Changes" section is added to the release notes
            "preset": "angular"
        }],

        // Update the changelog
        [
            "@semantic-release/changelog",
            {
                changelogFile: "CHANGELOG.md",
                changelogTitle: "# Changelog",
            }
        ],

        // Make a git commit, tag, and push the changes
        [
            "@semantic-release/git",
            {
                assets: [
                    "CHANGELOG.md",
                    "java/pom.xml",
                    "infra/charts/**/*.*",
                    "ui/package.json",
                    "sdk/python/feast/ui/package.json",
                    "sdk/python/feast/ui/yarn.lock"
                ],
                message: "chore(release): release ${nextRelease.version}\n\n${nextRelease.notes}"
            }
        ],

        // Publish a GitHub release (but don't spam issues/PRs with comments)
        [
            "@semantic-release/github",
            {
                successComment: false,
                failComment: false,
                failTitle: false,
                labels: false,
            }
        ],

        // For some reason all patches are tagged as pre-release. This step undoes that.
        ["@semantic-release/exec", {
            "publishCmd": "python ./infra/scripts/release/unset_prerelease.py ${nextRelease.version}"
        }],
    ]
}
