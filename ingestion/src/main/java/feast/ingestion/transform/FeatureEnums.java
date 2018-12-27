package feast.ingestion.transform;

public class FeatureEnums {
    public enum InputSource {
        FILE,
        BIGQUERY,
        PUBSUB,
        KAFKA
    }

    public enum FileFormat {
        CSV,
        JSON
    }
}