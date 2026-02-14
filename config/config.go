package config

type PluginConfig struct {
	DriverName     string
	DataSourceName string
}

func NewPluginConfig() PluginConfig {
	return PluginConfig{}
}
