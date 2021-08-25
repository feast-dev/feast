# Deploy a feature store

The Feast CLI can be used to deploy a feature store to your infrastructure, spinning up any necessary persistent resources like buckets or tables in data stores. The deployment target and effects depend on the `provider` that has been configured in your [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md) file, as well as the feature definitions found in your feature repository.

{% hint style="info" %}
Here we'll be using the example repository we created in the previous guide, [Create a feature store](create-a-feature-repository.md). You can re-create it by running `feast init` in a new directory.
{% endhint %}

## Deploying

To have Feast deploy your infrastructure, run `feast apply` from your command line while inside a feature repository:

```bash
feast apply

# Processing example.py as example
# Done!
```

Depending on whether the feature repository is configured to use a `local` provider or one of the cloud providers like `GCP` or `AWS`, it may take from a couple of seconds to a minute to run to completion.

{% hint style="warning" %}
At this point, no data has been materialized to your online store. Feast apply simply registers the feature definitions with Feast and spins up any necessary infrastructure such as tables. To load data into the online store, run `feast materialize`. See [Load data into the online store](load-data-into-the-online-store.md) for more details.
{% endhint %}

## Cleaning up

If you need to clean up the infrastructure created by `feast apply`, use the `teardown` command.

{% hint style="danger" %}
Warning: `teardown` is an irreversible command and will remove all feature store infrastructure. Proceed with caution!
{% endhint %}

```text
feast teardown
```

\*\*\*\*

