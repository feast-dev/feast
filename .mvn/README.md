Project-specific Maven Configuration
====================================

<https://maven.apache.org/configure.html>

maven.config
------------

Options to always give to the `mvn` CLI. This file doesn't support comments,
which is largely the reason this README exists.

`--also-make` tells Maven to use [the Reactor] to recognize inter-module
dependencies and include them in the build whenever necessary. This is generally
desirable in a multi-module project.

In addition to saving typing on the command line, using `--also-make` as a
default gets IntelliJ's main build & run functions working without failing to
resolve the inter-module deps or [running afoul of the Enforcer plugin][1], as
long as you use the ["Delegate IDE build/run actions to Maven"][2] setting.

[the Reactor]: https://maven.apache.org/guides/mini/guide-multiple-modules.html
[1]: https://maven.apache.org/enforcer/enforcer-rules/reactorModuleConvergence.html
[2]: https://www.jetbrains.com/help/idea/delegate-build-and-run-actions-to-maven.html
