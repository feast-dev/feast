// Release script for semantic-release.gitbook.io

const execSync = require("child_process").execSync;

// We have to dynamically generate all the supported branches for Feast because we use the `vA.B-branch` pattern for
// maintenance branches
MAJOR_VERSION_MAX=0
MINOR_VERSION_MIN=15
MINOR_VERSION_MAX=50

possible_branches = []
possible_branches.push({
    name: "master"
})

for (let step_major = 0; step_major <= MAJOR_VERSION_MAX; step_major++) {
    for (let step_minor = MINOR_VERSION_MIN; step_minor <= MINOR_VERSION_MAX; step_minor++) {
        possible_branches.push({name: `v${step_major}.${step_minor}-branch`})
    }
}

// Get the current branch (we want to validate that the correct kind of release is being created)
const branch = execSync("git rev-parse --abbrev-ref HEAD").toString("utf8");

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
            "verifyReleaseCmd": "./infra/scripts/validate-release.sh ${nextRelease.type} " + branch
        }]
    ]
}
