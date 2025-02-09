# What is a Feature Store?

*January 21, 2021* | *Willem Pienaar & Mike Del Balso*

Blog co-authored with Mike Del Balso, Co-Founder and CEO of Tecton, and cross-posted [here](https://www.tecton.ai/blog/what-is-a-feature-store/)

Data teams are starting to realize that operational machine learning requires solving data problems that extend far beyond the creation of data pipelines. In [Why We Need DevOps for ML Data](https://www.tecton.ai/blog/devops-ml-data/), Tecton highlighted some of the key data challenges that teams face when productionizing ML systems:

* Accessing the right raw data
* Building features from raw data
* Combining features into training data
* Calculating and serving features in production
* Monitoring features in production

Production data systems, whether for large scale analytics or real-time streaming, aren't new. However, *operational machine learning* — ML-driven intelligence built into customer-facing applications — is new for most teams. The challenge of deploying machine learning to production for operational purposes (e.g. recommender systems, fraud detection, personalization, etc.) introduces new requirements for our data tools.

A new kind of ML-specific data infrastructure is emerging to make that possible. Increasingly Data Science and Data Engineering teams are turning towards feature stores to manage the data sets and data pipelines needed to productionize their ML applications. This post describes the key components of a modern feature store and how the sum of these parts act as a force multiplier on organizations, by reducing duplication of data engineering efforts, speeding up the machine learning lifecycle, and unlocking a new kind of collaboration across data science teams.

Quick refresher: in ML, a feature is data used as an input signal to a predictive model. For example, if a credit card company is trying to predict whether a transaction is fraudulent, a useful feature might be *whether the transaction is happening in a foreign country*, or *how the size of this transaction compares to the customer's typical transaction*. When we refer to a feature, we're usually referring to the concept of that signal (e.g. "transaction_in_foreign_country"), not a specific value of the feature (e.g. not "transaction #1364 was in a foreign country").

## Enter the feature store

*"The interface between models and data"*

We first introduced feature stores in our blog post describing Uber's [Michelangelo](https://eng.uber.com/michelangelo-machine-learning-platform/) platform. Feature stores have since emerged as a necessary component of the operational machine learning stack.

Feature stores make it easy to:
1. Productionize new features without extensive engineering support
2. Automate feature computation, backfills, and logging
3. Share and reuse feature pipelines across teams
4. Track feature versions, lineage, and metadata
5. Achieve consistency between training and serving data
6. Monitor the health of feature pipelines in production

Feature stores aim to solve the full set of data management problems encountered when building and operating operational ML applications. A feature store is an ML-specific data system that:

* Runs data pipelines that transform raw data into feature values
* Stores and manages the feature data itself, and
* Serves feature data consistently for training and inference purposes

Feature stores bring economies of scale to ML organizations by enabling collaboration. When a feature is registered in a feature store, it becomes available for immediate reuse by other models across the organization. This reduces duplication of data engineering efforts and allows new ML projects to bootstrap with a library of curated production-ready features.

## Components of a Feature Store

There are 5 main components of a modern feature store: Transformation, Storage, Serving, Monitoring, and Feature Registry.

In the following sections we'll give an overview of the purpose and typical capabilities of each of these sections.

## Serving

Models need access to fresh feature values for inference. Feature stores accomplish this by regularly recomputing features on an ongoing basis. Transformation jobs are orchestrated to ensure new data is processed and turned into fresh new feature values. These jobs are executed on data processing engines (e.g. Spark or Pandas) to which the feature store is connected.

Model development introduces different transformation requirements. When iterating on a model, new features are often engineered to be used in training datasets that correspond to historical events (e.g. all purchases in the past 6 months). To support these use cases, feature stores make it easy to run "backfill jobs" that generate and persist historical values of a feature for training. Some feature stores automatically backfill newly registered features for preconfigured time ranges for registered training datasets.

Transformation code is reused across environments preventing training-serving skew and frees teams from having to rewrite code from one environment to the next. Feature stores manage all feature-related resources (compute, storage, serving) holistically across the feature lifecycle. Automating repetitive engineering tasks needed to productionize a feature, they enable a simple and fast path-to-production. Management optimizations (e.g. retiring features that aren't being used by any models, or deduplicating feature transformations across models) can bring significant efficiencies, especially as teams grow increasingly the complexity of managing features manually.

## Monitoring

When something goes wrong in an ML system, it's usually a data problem. Feature stores are uniquely positioned to detect and surface such issues. They can calculate metrics on the features they store and serve that describe correctness and quality. Feature stores monitor these metrics to provide a signal of the overall health of an ML application.

Feature data can be validated based on user defined schemas or other structural criteria. Data quality is tracked by monitoring for drift and training-serving skew. E.g. feature data served to models are compared to data on which the model was trained to detect inconsistencies that could degrade model performance.

When running production systems, it's also important to monitor operational metrics. Feature stores track operational metrics relating to core functionality. E.g. metrics relating to feature storage (availability, capacity, utilization, staleness) or feature serving (throughput, latency, error rates). Other metrics describe the operations of important adjacent system components. For example, operational metrics for external data processing engines (e.g. job success rate, throughput, processing lag and rate).

Feature stores make these metrics available to existing monitoring infrastructure. This allows ML application health to be monitored and managed with existing observability tools in the production stack. Having visibility into which features are used by which models, feature stores can automatically aggregate alerts and health metrics into views relevant to specific users, models, or consumers.

It's not essential that all feature stores implement such monitoring internally, but they should at least provide the interfaces into which data quality monitoring systems can plug. Different ML use cases can have different, specialized monitoring needs so pluggability here is important.

## Registry

A critical component in all feature stores is a centralized registry of standardized feature definitions and metadata. The registry acts as a single source of truth for information about a feature in an organization.

The registry is a central interface for user interactions with the feature store. Teams use the registry as a common catalog to explore, develop, collaborate on, and publish new definitions within and across teams.

The registry allows for important metadata to be attached to feature definitions. This provides a route for tracking ownership, project or domain specific information, and a path to easily integrate with adjacent systems. This includes information about dependencies and versions which is used for lineage tracking.

To help with common debugging, compliance, and auditing workflows, the registry acts as an immutable record of what's available analytically and what's actually running in production.

## Where to go to get started

We see features stores as the heart of the data flow in modern ML applications. They are quickly proving to be [critical infrastructure](https://a16z.com/2020/10/15/the-emerging-architectures-of-modern-data/) for data science teams putting ML into production. We expect 2021 to be a year of massive feature store adoption, as machine learning becomes a key differentiator for technology companies.

There are a few options for getting started with feature stores:

* [Feast](https://feastsite.wpenginepowered.com/) is a great option if you already have transformation pipelines to compute your features, but need a great storage and serving layer to help you use them in production. Feast is GCP/AWS only today, but we're working hard to make Feast available as a light-weight feature store for all environments. Stay tuned.
