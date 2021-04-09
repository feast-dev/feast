# Deploy a feature store

The Feast CLI can be used to deploy a feature store to your infrastructure. The deployment target depends on the `provider` that has been configured and the feature definitions found in your feature repository.

{% hint style="info" %}
Here we'll be using the example repository we created in the previous guide, [Create a feature store](create-a-feature-repository.md). You can re-create it by running `feast init` in a new directory.
{% endhint %}

## Deploying

To have Feast deploy your infrastructure, you can just run `feast apply` from the feature repository:

```bash
feast apply
Processing example.py as example
Done!
```

Depending on whether the feature repository is configured to use a `local` provider or one of the cloud providers like `GCP` or `AWS`, it may take from a couple of seconds to a minute to run to completion.

## Cleaning up

In order to clean up all infrastructure created by `feast apply`, it is possible to use the `teardown` method. Run the following command from your feature repository.

{% hint style="danger" %}
Teardown is an irreversible command and will remove all feature store infrastructure
{% endhint %}

```text
feast teardown
```

 

\*\*\*\*





