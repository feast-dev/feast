---
title: The Future of Feast
description: A look at Feast's journey, its evolution as a feature store, and the exciting path ahead with new maintainers and community-driven development.
date: 2024-02-15
authors: ["Willem Pienaar"]
---

<div class="hero-image">
  <img src="/images/blog/rocket.png" alt="The Road to Feast 1.0" loading="lazy">
</div>

# The Future of Feast

AI has taken center stage with the rise of large language models, but production ML systems remain the lifeblood of most AI powered companies today. At the heart of these products are feature stores like Feast, serving real-time, batch, and streaming data points to ML models.

I’d like to spend a moment taking stock on what we’ve accomplished over the last six years and what the growing Feast community has to look forward to.

**Act 1: Gojek and Google**

Feast was started in 2018 as a [collaboration](https://cloud.google.com/blog/products/ai-machine-learning/introducing-feast-an-open-source-feature-store-for-machine-learning) between Gojek and our friends at Google Cloud. The primary motivation behind the project was to reign in the rampant duplication of feature engineering across the Southeast Asian decacorn’s many ML teams.

Almost immediately, the key challenge with feature stores became clear: Can it be generalized across various ML use cases?

The natural way to answer that question is to battle test the software out in the open. So in late 2018, spurred on by our friends in the Kubeflow project, we open sourced Feast. A community quickly formed around the project. This group was mostly made up of software engineers at data rich technology companies, trying to find a way to help their ML teams productionize models at a much higher pace.

Having a community centric approach is in the DNA of the project. All of our RFCs, discussions, designs, community calls, and code are open source. The project became a vehicle for ML platform teams globally to collaborate. Many teams saw the project as a means of stress testing their internal feature store designs, while others like Agoda, Zulily, Farfetch, and Postmates adopted the project wholesale and became core contributors.

As time went by the demand grew for the project to have neutral ownership and formal governance. This led to us [entering the project into the Linux Foundation for AI in 2020](https://lfaidata.foundation/blog/2020/11/10/feast-joins-lf-ai-data-as-new-incubation-project/).

**Act 2: Rise of the Feature Store**

By 2020, the demand for feature stores had reached a fever pitch. If you were dealing with more than just an Excel sheet of data, you were likely planning to either build or buy a feature store. A category formed around feature stores and MLOps.

Being a neutrally governed open source project brought in a raft of contributions, which helped the project generalize not just to different data platforms and vendors, but also different use cases and deployment patterns. A few of the highlights include:

*   We worked closely with AI teams at [Snowflake](https://quickstarts.snowflake.com/guide/getting_started_with_feast_snowflake/index.html#0), [Azure](https://techcommunity.microsoft.com/t5/ai-customer-engineering-team/bringing-feature-store-to-azure-from-microsoft-azure-redis-and/ba-p/2918917), [GCP](https://cloud.google.com/blog/products/databases/getting-started-with-feast-on-google-cloud), and [Redis](https://redis.com/blog/building-feature-stores-with-redis-introduction-to-feast-with-redis/) to bring the project up to cloud scale and support their customers.
*   Data tooling providers contributed connectors and functionality to the project, namely [DataStax](https://www.datastax.com/blog/lift-your-mlops-pipeline-to-the-cloud-with-feast-and-astra-db), [Bytewax](https://bytewax.io/blog/real-time-ml), [Dragonfly](https://www.dragonflydb.io/blog/running-the-feast-feature-store-with-dragonfly), [Flyte](https://docs.flyte.org/en/latest/flytesnacks/examples/feast_integration/index.html), [Arize](https://docs.arize.com/arize/resources/integrations/feast), [DataHub](https://datahubproject.io/docs/generated/ingestion/sources/feast/), and [WhyLabs](https://docs.whylabs.ai/docs/feast-integration/).
*   ML projects and vendors integrated the feature store into their offerings at [AWS](https://aws.amazon.com/blogs/opensource/getting-started-with-feast-an-open-source-feature-store-running-on-aws-managed-services/), [Kubeflow](https://www.kubeflow.org/docs/external-add-ons/feature-store/overview/), [Valohai](https://docs.valohai.com/hc/en-us/articles/19656452332177-Integrating-with-Feast), [ZenML](https://www.zenml.io/integrations/feast), [Rockset](https://rockset.com/blog/rockset-and-feast-feature-store-real-time-machine-learning/) and [TFX](https://blog.tensorflow.org/2023/02/extend-your-tfx-pipeline-with-tfx-addons.html)

It’s also important to mention that by far the biggest contributor to Feast was [Tecton](https://www.tecton.ai/?__hstc=145182251.7e8cfcb692e269eec7caf34133c3f069.1742093344132.1742093344132.1742093344132.1&__hssc=145182251.1.1742093344132&__hsfp=2836145088), who invested considerable resources into the project and helped create the category.

Today, the project is battle hardened and stable. It’s seen adoption and/or contribution from companies like Adyen, Affirm, Better, Cloudflare, Discover, Experian, Lowes, Red Hat, Robinhood, Palo Alto Networks, Porch, Salesforce, Seatgeek, Shopify, and Twitter, just to name a few.

**Act 3: The Road to 1.0**

The rate of change in AI has accelerated, and nowhere is it moving faster than in open source. Keeping up with this rate of change for AI infra requires the best minds, so with that we’d like to introduce a set of contributors who will be graduating to official project maintainers:

*   [Francisco Javier Arceo](https://www.linkedin.com/in/franciscojavierarceo/) – Engineering Manager, [Affirm](https://www.affirm.com/)
*   [Edson Tirelli](https://www.linkedin.com/in/edsontirelli/) – Sr Principal Software Engineer, [Red Hat](https://www.redhat.com/)
*   [Jeremy Ary](https://www.linkedin.com/in/jeremyary) – Sr Principal Software Engineer, Red Hat
*   [Shuchu Han](https://www.linkedin.com/in/shuchu/) – OSS Contributor, Independent
*   [Hao Xu](https://www.linkedin.com/in/hao-xu-a04436103/) – Lead Software Engineer, J.P. Morgan

Over the next few months these maintainers will focus on bringing the project to a major 1.0 release. In our next post we will take a closer look at what the road to 1.0 looks like.

If you’d like to get involved, try out the project [over at GitHub](https://github.com/feast-dev/feast) or join our [Slack](https://feastopensource.slack.com/) community!