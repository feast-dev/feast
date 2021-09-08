# Feast 0.9 vs Feast 0.10+

Feast 0.10 brought about major changes to the way Feast is architected and how the software is intended to be deployed, extended, and operated.

{% hint style="success" %}
Please see [Upgrading from Feast 0.9](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#) for a guide on how to upgrade to the latest Feast version.
{% endhint %}

### Changes introduced in Feast 0.10

Feast contributors identified various [design challenges](https://feast.dev/blog/a-state-of-feast/) in Feast 0.9 that made deploying, operating, extending, and maintaining it challenging. These challenges applied both to users and contributors.   
  
Our goal is to make ML practitioners immediately productive in operationalizing data for machine learning. To that end, Feast 0.10+ made the following improvements on Feast 0.9:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Challenges in Feast 0.9<b> (Before)</b>
      </th>
      <th style="text-align:left">Changed in Feast 0.10+ (After)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">Hard to install because it was a heavy-weight system with many components
        requiring a lot of configuration</td>
      <td style="text-align:left">
        <ul>
          <li>Easy to install via <code>pip install</code>
          </li>
          <li>Opinionated default configurations</li>
          <li>No Helm charts necessary</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Engineering support needed to deploy/operate reliably</td>
      <td style="text-align:left">
        <p></p>
        <ul>
          <li>Feast moves from a stack of services to a CLI/SDK</li>
          <li>No need for Kubernetes or Spark</li>
          <li>No long running processes or orchestrators</li>
          <li>Leverages globally available managed services where possible</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Hard to develop/debug with tightly coupled components, async operations,
        and hard to debug components like Spark</td>
      <td style="text-align:left">
        <ul>
          <li>Easy to develop and debug</li>
          <li>Modular components</li>
          <li>Clear extension points</li>
          <li>Fewer background operations</li>
          <li>Faster feedback</li>
          <li>Local mode</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Inability to benefit from cloud-native technologies because of focus on
        reusable technologies like Kubernetes and Spark</td>
      <td style="text-align:left">
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <ul>
          <li>Leverages best-in-class cloud technologies so users can enjoy scalable
            + powerful tech stacks without managing open source stacks themselves</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

### Changes in more detail

Where Feast 0.9 was a large stack of components that needed to be deployed to Kubernetes, Feast 0.10 is simply a lightweight SDK and CLI. It doesn’t need any long-running processes to operate. This SDK/CLI can deploy and configure your feature store to your infrastructure, and execute workflows like building training datasets or reading features from an online feature store.

* **Feast 0.10 introduces local mode:** Local mode allows users to try out Feast in a completely local environment \(without using any cloud technologies\). This provides users with a responsive means of trying out the software before deploying it into a production environment.
* **Feast comes with opinionated defaults:** As much as possible we are attempting to make Feast a batteries-included feature store that removes the need for users to configure infinite configuration options \(as with Feast 0.9\). Feast 0.10 comes with sane default configuration options to deploy Feast on your infrastructure.
* **Feast Core was replaced by a file-based \(S3, GCS\) registry:** Feast Core is a metadata server that maintains and exposes an API of feature definitions. With Feast 0.10, we’ve moved this entire service into a single flat file that can be stored on either the local disk or in a central object store like S3 or GCS. The benefit of this change is that users don’t need to maintain a database and a registry service, yet they can still access all the metadata they had before.
* **Materialization is a CLI operation:** Instead of having ingestion jobs be managed by a job service, users can now schedule a batch ingestion job themselves by calling “materialize”. This change was introduced because most teams already have schedulers like Airflow in their organization. By starting ingestion jobs from Airflow, teams are now able to easily track state outside of Feast and to debug failures synchronously. Similarly, streaming ingestion jobs can be launched through the “apply” command
* **Doubling down on data warehouses:** Most modern data teams are doubling down on data warehouses like BigQuery, Snowflake, and Redshift. Feast doubles down on these big data technologies as the primary interfaces through which it launches batch operations \(like training dataset generation\). This reduces the development burden on Feast contributors \(since they only need to reason about SQL\), provides users with a more responsive experience, avoids moving data from the warehouse \(to compute joins using Spark\), and provides a more serverless and scalable experience to users.
* **Temporary loss of streaming support:** Unfortunately, Feast 0.10, 0.11, and 0.12 do not support streaming feature ingestion out of the box. It is entirely possible to launch streaming ingestion jobs using these Feast versions, but it requires the use of a Feast extension point to launch these ingestion jobs. It is still a core design goal for Feast to support streaming ingestion, so this change is in the development backlog for the Feast project.
* **Addition of extension points:** Feast 0.10+ introduces various extension points. Teams can override all feature store behavior by writing \(or extending\) a provider. It is also possible for teams to add their own data storage connectors for both an offline and online store using a plugin interface that Feast provides.

### Comparison of architectures

#### Feast 0.9

![](../.gitbook/assets/image%20%289%29.png)

#### Feast 0.10, 0.11, and 0.12 architecture

![](../.gitbook/assets/image%20%2819%29.png)

#### Feast 1.0 architecture \(eventual goal\)

![](../.gitbook/assets/image%20%2821%29.png)

### Comparison of components

<table>
  <thead>
    <tr>
      <th style="text-align:left">Component</th>
      <th style="text-align:left">Feast 0.9</th>
      <th style="text-align:left">Feast 0.10, 011, 0.12+</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><b>Architecture</b>
      </td>
      <td style="text-align:left">
        <ul>
          <li>Service-oriented architecture</li>
          <li>Containers and services deployed to Kubernetes</li>
        </ul>
      </td>
      <td style="text-align:left">
        <ul>
          <li>SDK/CLI centric software</li>
          <li>Feast is able to deploy or configure infrastructure for use as a feature
            store</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Installation</b> 
      </td>
      <td style="text-align:left">Terraform and Helm</td>
      <td style="text-align:left">
        <ul>
          <li>Pip to install SDK/CLI</li>
          <li>Provider used to deploy Feast components to GCP, AWS, or other environments
            during <em><b>apply</b></em>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Required infrastructure</b>
      </td>
      <td style="text-align:left">Kubernetes, Postgres, Spark, Docker, Object Store</td>
      <td style="text-align:left">None</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Batch compute</b>
      </td>
      <td style="text-align:left">Yes (Spark based)</td>
      <td style="text-align:left">
        <ul>
          <li>Python native (client-side) for batch data loading</li>
          <li>Data warehouse for batch compute</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Streaming support</b>
      </td>
      <td style="text-align:left">Yes (Spark based)</td>
      <td style="text-align:left">Planned. Streaming jobs will be launched using <em><b>apply</b></em>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Offline store</b>
      </td>
      <td style="text-align:left">None (can source data from any source Spark supports)</td>
      <td style="text-align:left">BigQuery, Snowflake (planned), Redshift, or custom implementations</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Online store</b>
      </td>
      <td style="text-align:left">Redis</td>
      <td style="text-align:left">DynamoDB, Firestore, Redis, and more planned.</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Job Manager</b>
      </td>
      <td style="text-align:left">Yes</td>
      <td style="text-align:left">No</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Registry</b>
      </td>
      <td style="text-align:left">gRPC service with Postgres backend</td>
      <td style="text-align:left">File-based registry with accompanying SDK for exploration</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Local Mode</b>
      </td>
      <td style="text-align:left">No</td>
      <td style="text-align:left">Yes</td>
    </tr>
  </tbody>
</table>

### Upgrading from Feast 0.9 to the latest Feast

Please see the [Feast 0.9 Upgrade Guide](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#).

