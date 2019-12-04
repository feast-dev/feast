package feast.core.metrics.collector;

import feast.core.dao.FeatureSetRepository;
import feast.core.dao.StoreRepository;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.List;

/**
 * FeastResourceCollector exports metrics about Feast Resources.
 * <p>
 * For example: total number of registered feature sets and stores.
 */
public class FeastResourceCollector extends Collector {

  private final FeatureSetRepository featureSetRepository;
  private final StoreRepository storeRepository;

  public FeastResourceCollector(FeatureSetRepository featureSetRepository,
      StoreRepository storeRepository) {
    this.featureSetRepository = featureSetRepository;
    this.storeRepository = storeRepository;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> samples = new ArrayList<>();
    samples.add(new GaugeMetricFamily("feast_core_feature_set_total",
        "Total number of registered feature sets", featureSetRepository.count()));
    samples.add(new GaugeMetricFamily("feast_core_store_total",
        "Total number of registered stores", storeRepository.count()));
    return samples;
  }
}
