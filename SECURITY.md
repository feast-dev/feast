# Security Policy

The Feast community takes security bugs seriously, and we appreciate the effort it takes to find and report them. We follow [GitHub's coordinated disclosure process](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/about-coordinated-disclosure-of-security-vulnerabilities) so that a fix can be prepared before details become public.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub, using **[Report a vulnerability](https://github.com/feast-dev/feast/security/advisories/new)** on this repository's Security tab. Only the maintainers can see the report, and you will be credited on the published advisory if you would like to be.

Before reporting, please check the [published advisories](https://github.com/feast-dev/feast/security/advisories) to confirm the issue has not already been addressed.

A report needs to show a clear, reproducible security impact. Please include:

- the affected version or commit, and the configuration involved
- a proof of concept, or steps that reproduce the issue
- the actual impact, rather than a theoretical concern

Raw scanner or dependency-audit output does not meet that bar on its own, since it does not establish that the issue is reachable in Feast. Reports that have not been manually verified against Feast, including bulk, automated, or AI-generated submissions, may be closed without further response.

> [!WARNING]
> Do not open a public GitHub issue, pull request, or Slack message for a security vulnerability. Those are visible to everyone and disclose the problem before a fix exists.

For anything that is not a vulnerability, including hardening suggestions and questions about how Feast's authentication and authorization work, a normal [GitHub issue](https://github.com/feast-dev/feast/issues) is the right place.

## Supported versions

Security fixes are applied to the latest release. Feast releases roughly monthly and offers best-effort community support, as described in the [versioning policy](docs/project/versioning-policy.md); there is no long-term support branch, so upgrading to the current release is the supported way to receive a fix.

## Published advisories

Past advisories for this project are listed under [Security advisories](https://github.com/feast-dev/feast/security/advisories).
