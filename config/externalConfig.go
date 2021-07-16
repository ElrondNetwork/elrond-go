package config

// ExternalConfig will hold the configurations for external tools, such as Explorer or Elastic Search
type ExternalConfig struct {
	ElasticSearchConnector ElasticSearchConfig
	EventNotifierConnector EventNotifierConfig
}

// ElasticSearchConfig will hold the configuration for the elastic search
type ElasticSearchConfig struct {
	Enabled          bool
	IndexerCacheSize int
	URL              string
	UseKibana        bool
	Username         string
	Password         string
	EnabledIndexes   []string
}

// EventNotifierConfig will hold the configuration for the events notifier driver
type EventNotifierConfig struct {
	Enabled          bool
	UseAuthorization bool
	ProxyUrl         string
	Username         string
	Password         string
}
