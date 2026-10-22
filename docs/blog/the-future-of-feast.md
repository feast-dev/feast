# The Future of Feast

*February 23, 2024* | *Willem Pienaar*

AI has taken center stage with the rise of large language models, but production ML systems remain the lifeblood of most AI powered companies today. At the heart of these products are feature stores like Feast, serving real-time, batch, and streaming data points to ML models. I'd like to spend a moment taking stock on what we've accomplished over the last six years and what the growing Feast community has to look forward to.

## Act 1: Gojek and Google

Feast was started in 2018 as a [collaboration](https://cloud.google.com/blog/products/ai-machine-learning/introducing-feast-an-open-source-feature-store-for-machine-learning) between Gojek and our friends at Google Cloud. The primary motivation behind the project was to reign in the rampant duplication of feature engineering across the Southeast Asian decacorn's many ML teams.

Almost immediately, the key challenge with feature stores became clear: Can it be generalized across various ML use cases?

The natural way to answer that question is to battle test the software out in the open. So in late 2018, spurred on by our friends in the Kubeflow project, we open sourced Feast. A community quickly formed around the project. This group was mostly made up of software engineers at data rich technology companies, trying to find a way to help their ML teams productionize models at a much higher pace.

Having a community centric approach is in the DNA of the project. All of our RFCs, discussions, designs, community calls, and code are open source. The project became a vehicle for ML platform teams globally to collaborate. Many teams saw the project as a means of stress testing their internal feature store designs, while others like Agoda, Zulily, Farfetch, and Postmates adopted the project wholesale and became core contributors.

As time went by the demand grew for the project to have neutral ownership and formal governance. This led to us [entering the project into the Linux Foundation for AI in 2020](https://lfaidata.foundation/blog/2020/11/10/feast-joins-lf-ai-data-as-new-incubation-project/).

## Act 2: Rise of the Feature Store

By 2020, the demand for feature stores had reached a fever pitch. If you were dealing with more than just an Excel sheet of data, you were likely planning to either build or buy a feature store. A category formed around feature stores and MLOps. Being a neutrally governed open source project brought in a raft of contributions, which helped the project generalize not just to different data platforms and vendors, but also different use cases and deployment patterns. A few of the highlights include:

* We worked closely with AI teams at [Snowflake](https://quickstarts.snowflake.com/guide/getting_started_with_feast/), [Azure](https://techcommunity.microsoft.com/t5/ai-customer-engineering-team/using-feast-feature-store-with-azure-ml/ba-p/2908404)

It's also important to mention that by far the biggest contributor to Feast was [Tecton](https://www.tecton.ai/), who invested considerable resources into the project and helped create the category.

Today, the project is battle hardened and stable. It's seen adoption and/or contribution from companies like Adyen, Affirm, Better, Cloudflare, Discover, Experian, Lowes, Red Hat, Robinhood, Palo Alto Networks, Porch, Salesforce, Seatgeek, Shopify, and Twitter, just to name a few.

## Act 3: The Road to 1.0

The rate of change in AI has accelerated, and nowhere is it moving faster than in open source. Keeping up with this rate of change for AI infra requires the best minds, so with that we'd like to introduce a set of contributors who will be graduating to official project maintainers:

* [Francisco Javier Arceo](https://www.linkedin.com/in/franciscojavierarceo/) – Engineering Manager, [Affirm](https://www.affirm.com/)
* [Hao Xu](https://www.linkedin.com/in/hao-xu-a04436103/) – Lead Software Engineer, J.P. Morgan

Over the next few months these maintainers will focus on bringing the project to a major 1.0 release. In our next post we will take a closer look at what the road to 1.0 looks like.

If you'd like to get involved, try out the project [over at GitHub](https://github.com/feast-dev/feast) or join our [Slack](https://feastopensource.slack.com) community!
