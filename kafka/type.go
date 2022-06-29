package kafka

type KafkaConfig struct {
	Brokers string `json:"brokers"`
	Group   string `json:"group"`
}
