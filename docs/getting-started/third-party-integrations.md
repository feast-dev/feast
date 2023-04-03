# Third party integrations

We integrate with a wide set of tools and technologies so you can make Feast work in your existing stack. Many of these integrations are maintained as plugins to the main Feast repo.

{% hint style="info" %}
Don't see your offline store or online store of choice here? Check out our guides to make a custom one!

* [Adding a new offline store](../how-to-guides/customizing-feast/adding-a-new-offline-store.md)
* [Adding a new online store](../how-to-guides/customizing-feast/adding-support-for-a-new-online-store.md)
{% endhint %}

## Integrations

See [Functionality and Roadmap](../roadmap.md)

## Standards

In order for a plugin integration to be highlighted, it must meet the following requirements:

1. The plugin must have tests. Ideally it would use the Feast universal tests (see this [guide](../how-to-guides/adding-or-reusing-tests.md) for an example), but custom tests are fine.
2. The plugin must have some basic documentation on how it should be used.
3. The author must work with a maintainer to pass a basic code review (e.g. to ensure that the implementation roughly matches the core Feast implementations).

In order for a plugin integration to be merged into the main Feast repo, it must meet the following requirements:

1. The PR must pass all integration tests. The universal tests (tests specifically designed for custom integrations) must be updated to test the integration.
2. There is documentation and a tutorial on how to use the integration.
3. The author (or someone else) agrees to take ownership of all the files, and maintain those files going forward.
4. If the plugin is being contributed by an organization, and not an individual, the organization should provide the infrastructure (or credits) for integration tests.
