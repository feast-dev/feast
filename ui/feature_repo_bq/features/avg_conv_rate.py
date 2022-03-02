from feast import BigQuerySource

avg_conv_rate_source = BigQuerySource(
    name="avg_conv_rate", table_ref="kf-feast.danny_test_dataset.avg_conv_rate"
)
