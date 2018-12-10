export default {
  formatTimestamp (timestamp) {
    let datetime = new Date(timestamp);
    return datetime.toLocaleTimeString('en-US');
  },
  fillGranularity (feature) {
    if (typeof feature.spec['granularity'] === "undefined") {
      feature.spec['granularity'] = "NONE";
    }
    return feature;
  }
}

