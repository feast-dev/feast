package metrics

// StatsdClient wraps DogStatsD so tests can inject a fake.
// The real github.com/DataDog/datadog-go/v5/statsd.Client satisfies this interface.
type StatsdClient interface {
	Count(name string, value int64, tags []string, rate float64) error
	Distribution(name string, value float64, tags []string, rate float64) error
}

// NoOpStatsdClient does nothing when metrics are disabled.
type NoOpStatsdClient struct{}

func (n *NoOpStatsdClient) Count(string, int64, []string, float64) error          { return nil }
func (n *NoOpStatsdClient) Distribution(string, float64, []string, float64) error { return nil }
