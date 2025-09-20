package rm_cfg

type RabbitmqProducerCfg struct {
	ConnRetryDelayMills      int  `yaml:"conn-retry-delay-mills"` // auto reconnect delay in first retry
	MaxConnections           int  `yaml:"max-connections"`        // in 99% of cases, only considering 1 or 2, fewer connections, multiple channels, and IO multiplexing are the right choices.
	MaxChannelsPerConn       int  `yaml:"max-channels-per-conn"`
	RecoverMillsAfterSuspend int  `yaml:"recover-mills-after-suspend"`
	EnableStats              bool `yaml:"enable-stats"` // enable pool statistics
	EnableConfirm            bool `yaml:"enable-confirm"`
	ConfirmQueueCap          int  `yaml:"confirm-queue-cap"` // default 100
	EnableMandatory          bool `yaml:"enable-mandatory"`
}

type ConsumeApplyCfgItem struct {
	Queue       string         `yaml:"queue"`        // the queue of subscribe
	Concurrency int            `yaml:"concurrency"`  // number of concurrent subscription queues
	PrefetchCnt int            `yaml:"prefetch-cnt"` // prefetch count
	AutoAck     bool           `yaml:"auto-ack"`
	ConsumeArgs map[string]any `yaml:"consume-args"`
}

type RabbitmqConsumerCfg struct {
	ConnRetryDelayMills  int                   `yaml:"conn-retry-delay-mills"` // auto reconnect delay in first retry
	MaxConnections       int                   `yaml:"max-connections"`
	ConsumeApplyCfgItems []ConsumeApplyCfgItem `yaml:"consume-apply-cfg-items"`
}

type RabbitmqCfg struct {
	Addresses   string              `yaml:"addresses"`    // single: 127.0.0.1:5672, cluster: 192.168.1.3:5672,192.168.1.2:5672,192.168.1.3:5672
	VirtualHost string              `yaml:"virtual-host"` // default '/'
	Username    string              `yaml:"username"`
	Password    string              `yaml:"password"`
	LogLevel    string              `yaml:"log-level"` // silence, trace, debug, info, warn, error, fatal
	ProducerCfg RabbitmqProducerCfg `yaml:"producer-cfg"`
	ConsumerCfg RabbitmqConsumerCfg `yaml:"consumer-cfg"`
}
