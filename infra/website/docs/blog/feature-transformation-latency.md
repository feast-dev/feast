---
title: "Faster On Demand Transformations in Feast 🏎️💨"
description: "Discover how Feast's new Native Python Mode and transformations on writes reduce feature transformation latency by up to 10x, enabling faster and more efficient feature engineering workflows."
date: 2025-01-14
authors: ["Francisco Javier Arceo", "Shuchu Han"]
---

<div class="hero-image">
  <img src="/images/blog/latency-comparison.png" alt="Pandas vs Native Python Latency Decomposition" loading="lazy">
</div>

# Faster On Demand Transformations in Feast 🏎️💨

*Thank you to Shuchu Han and Francisco Javier Arceo for their contributions [evaluating the latency](https://github.com/feast-dev/feast/issues/4207) of transformation for On Demand Feature Views and adding [transformations on writes to On Demand Feature Views](https://github.com/feast-dev/feast/issues/4376). Thank you also to Maks Stachowiak, Ross Briden, Ankit Nadig, and the folks at Affirm for inspiring this work and creating an initial proof of concept.*

Feature engineering is at the core of building high-performance machine learning models. The Feast team has introduced two major enhancements to [On Demand Feature Views](https://docs.feast.dev/reference/beta-on-demand-feature-view) (ODFVs), pushing the boundaries of efficiency and flexibility for data scientists and engineers. Here's a closer look at these exciting updates:

## 1. Turbocharging Transformations with Native Python Mode

Traditionally, transformations in ODFVs were limited to Pandas-based operations. While powerful, Pandas transformations can be computationally expensive for certain use cases. Feast now introduces **Native Python Mode**, a feature that allows users to write transformations using pure Python.

Key benefits of Native Python Mode include:

* **Blazing Speed:** Transformations using Native Python are nearly **10x faster** compared to Pandas for many operations.
* **Intuitive Design:** This mode supports list-based and singleton (row-level) transformations, making it easier for data scientists to think in terms of individual rows rather than entire datasets.
* **Versatility:** Users can now switch between batch and singleton transformations effortlessly, catering to both historical and online retrieval scenarios.

Here's an example of using Native Python Mode for singleton transformations:

```python
@on_demand_feature_view(
    sources=[driver_hourly_stats_view, input_request],
    schema=[Field(name="conv_rate_plus_acc_singleton", dtype=Float64)],
    mode="python",
    singleton=True,
)
def transformed_conv_rate_singleton(inputs: Dict[str, Any]) -> Dict[str, Any]:
    return {"conv_rate_plus_acc_singleton": inputs["conv_rate"] + inputs["acc_rate"]}
```

This approach aligns with how many data scientists naturally process data, simplifying the implementation of feature engineering workflows.

## 2. Transformations on Writes: A New Dimension of Latency Optimization

Until now, ODFVs operated solely as **transformations on reads**, applying logic during online feature retrieval. While this ensured flexibility, it sometimes came at the cost of increased latency during retrieval. Feast now supports **transformations on writes**, enabling users to apply transformations during data ingestion and store the transformed features in the online store.

Why does this matter?

* **Reduced Online Latency:** With transformations pre-applied at ingestion, online retrieval becomes a straightforward lookup, significantly improving performance for latency-sensitive applications.
* **Operational Flexibility:** By toggling the `write_to_online_store` parameter, users can choose whether transformations should occur at write time (to optimize reads) or at read time (to preserve data freshness).

Here's an example of applying transformations during ingestion:

```python
@on_demand_feature_view(
    sources=[driver_hourly_stats_view],
    schema=[Field(name="conv_rate_adjusted", dtype=Float64)],
    mode="pandas",
    write_to_online_store=True,  # Apply transformation during write time
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_adjusted"] = features_df["conv_rate"] * 1.1
    return df
```

With this new capability, data engineers can optimize online retrieval performance without sacrificing the flexibility of on-demand transformations.

## The Future of ODFVs and Feature Transformations

These enhancements bring ODFVs closer to the goal of seamless feature engineering at scale. By combining high-speed Python-based transformations with the ability to optimize retrieval latency, Feast empowers teams to build more efficient, responsive, and production-ready feature pipelines.

For more detailed examples and use cases, check out the [documentation for On Demand Feature Views](https://docs.feast.dev/reference/beta-on-demand-feature-view). Whether you're a data scientist prototyping features or an engineer optimizing a production system, the new ODFV capabilities offer the tools you need to succeed.

The future of Feature Transformations in Feast will be to unify feature transformations and feature views to allow for a simpler API. If you have thoughts or interest in giving feedback to the maintainers, feel free to comment directly on [the GitHub Issue](https://github.com/feast-dev/feast/issues/4584) or in [the RFC](https://docs.google.com/document/d/1KXCXcsXq1bUvbSpfhnUjDSsu4HpuUZ5XiZoQyltCkvo/edit?tab=t.0#heading=h.g6j9jyjkf5sm).

Our goal is to make Feast blazing fast for feature transformation and serving.

**Are you ready to unlock the full potential of On Demand Feature Views? Update your Feast repository and start building today!**
