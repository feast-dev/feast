# Apply

The `apply` command is used to persist your feature definitions in the feature registry and to configure or provision the necessary infrastructure for your feature store to operate

### What happens during `feast apply`

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

