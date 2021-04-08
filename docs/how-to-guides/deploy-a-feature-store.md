# Deploy a feature store

After creating a Feature Repository, we use Feast CLI to create all required infrastructure to serve the features we defined there.

{% hint style="info" %}
Here we'll be using the example repository we created in the previous guide, [Create a feature store](create-a-feature-repository.md). You can re-create it by running `feast init` in a new directory.
{% endhint %}

## Deploying

To have Feast create all infrastructure, you can just run `feast apply` in the Feature Repository directory. It should be a pretty straightforward process:

```
$ feast.py
Processing example.py as example
Done!
```

Depending on whether the Feature Repository is configured to use the Local provider or one of the cloud providers like GCP or AWS, it may take from a couple of seconds to a minute.

## What happens during `feast apply`

#### 1. Scan the Feature Repository

Feast will scan Python files in your Feature Repository, and find all Feast object definitions, such as Feature Views, Entities, and Data Sources. 

#### 2. Update metadata

If all definitions look valid, Feast will sync the metadata about Feast objects to the Metadata Store. Metadata store is a tiny database storing most of the same information you have in the Feature Repository, plus some state in a more structured form. It is necessary mostly because the production feature serving infrastructure won't be able to access Python files in the Feature Repository at run time, but it will be able to efficiently and securely read the feature definitions from the Metadata Store.

#### 3. Create cloud infrastructure

At this step, Feast CLI will create all necessary infrastructure for feature serving and materialization to work. What exactly gets created depends on what provider is configured to be used in `feature_store.yaml` in the Feature Repository. 

For example, for Local provider, it is as easy as creating a file on your local filesystem as a key-value store to serve feature data from. Local provider is most usable for local testing, no real production serving happens there.

A more interesting configuration is when we're configured Feast to use GCP provider and Cloud Datastore to store feature data. When you run `feast apply`, Feast will make sure you have valid credentials and create some metadata objects in the Datastore for each Feature View.

Similarly, when using AWS, Feast will make sure that resources like DynamoDB tables are created for every Feature View.

{% hint style="warning" %}
Since `feast deploy` \(when configured to use non-Local provider\) will create cloud infrastructure in your AWS or GCP account, it may incur some costs on your cloud bill. While we aim to design it in a way that Feast cloud resources don't cost much when not serving features, preferring "serverless" cloud services that bill per request, please refer to the specific Provider documentation to make sure there are no surprises.
{% endhint %}

## Cleaning up

If you no longer need the infrastructure, you can run `feast destroy` to clean up. **Note that this will irrevocably delete all data in the online store, so use it with care.**

\*\*\*\*





