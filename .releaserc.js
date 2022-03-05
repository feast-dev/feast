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
        "@semantic-release/commit-analyzer",
        "@semantic-release/release-notes-generator",
        "@semantic-release/github",
        [
            "@semantic-release/changelog",
            {
                changelogFile: "CHANGELOG.md"
            }
        ],
        [
            "@semantic-release/git",
            {
                assets: [
                    "CHANGELOG.md"
                ],
                message: "chore(release): release ${nextRelease.version}\n\n${nextRelease.notes}"
            }
        ],
        ["@semantic-release/exec", {
            "verifyReleaseCmd": "./infra/scripts/validate-release.sh ${nextRelease.type} " + current_branch
        }]
    ]
}


