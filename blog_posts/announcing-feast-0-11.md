# Announcing Feast 0.11

*June 23, 2021* | *Jay Parthasarthy & Willem Pienaar*

Feast 0.11 is here! This is the first release after the major changes introduced in Feast 0.10. We've focused on two areas in particular:

1. Introducing a new online store, Redis, which supports feature serving at high throughput and low latency.
2. Improving the Feast user experience through reduced boilerplate, smoother workflows, and improved error messages. A key addition here is the introduction of *feature inferencing,* which allows Feast to dynamically discover data schemas in your source data.

Let's get into it!

### Support for Redis as an online store 🗝

Feast 0.11 introduces support for Redis as an online store, allowing teams to easily scale up Feast to support high volumes of online traffic. Using Redis with Feast is as easy as adding a few lines of configuration to your feature_store.yaml file:

```yaml
project: fraud
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: localhost:6379
```

Feast is then able to read and write from Redis as its online store.

```bash
$ feast materialize

Materializing 3 feature views to 2021-06-15 18:43:03+00:00 into the redis online store.

user_account_features from 2020-06-16 18:43:04 to 2021-06-15 18:43:13:
100%|███████████████████████| 9944/9944 [00:04<00:00, 20065.15it/s]
user_transaction_count_7d from 2021-06-08 18:43:21 to 2021-06-15 18:43:03:
100%|███████████████████████| 9674/9674 [00:04<00:00, 19943.82it/s]
```

We're also working on making it easier for teams to add their own storage and compute systems through plugin interfaces. Please see this RFC for more details on the proposal.

### Feature Inferencing 🔎

Before 0.11, users had to define each feature individually when defining Feature Views. Now, Feast infers the schema of a Feature View based on upstream data sources, significantly reducing boilerplate.

Before:
```python
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
    ],
    input=BigQuerySource(table_ref="feast-oss.demo_data.driver_hourly_stats"),
)
```

Aside from these additions, a wide variety of small bug fixes, and UX improvements made it into this release. [Check out the changelog](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md) for a full list of what's new.

Special thanks and a big shoutout to the community contributors whose changes made it into this release: [MattDelac](https://github.com/MattDelac), [mavysavydav](https://github.com/mavysavydav), [szalai1](https://github.com/szalai1), [rightx2](https://github.com/rightx2)

### Help us design Feast for AWS 🗺️

The 0.12 release will include native support for AWS. We are looking to meet with teams that are considering using Feast to gather feedback and help shape the product as design partners. We often help our design partners out with architecture or design reviews. If this sounds helpful to you, [join us in Slack](http://slack.feastsite.wpenginepowered.com/), or [book a call with Feast maintainers here](https://calendly.com/d/gc29-y88c/feast-chat-w-willem).

### Feast from around the web 📣
