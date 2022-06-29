package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"strings"
)

type KafkaConsumer struct {
	conn     *kafka.Conn
	Listener map[string]func(topic, key, value string)
	ctx      context.Context
	config   *KafkaConfig
}

func (k *KafkaConsumer) New(config *KafkaConfig) {
	if config == nil {
		panic("config is nil")
	}

	k.ctx = context.Background()
	k.config = config
}

func (k *KafkaConsumer) Start() *KafkaConsumer {

	for topic, _ := range k.Listener {

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:               strings.Split(k.config.Brokers, ","),
			GroupID:               k.config.Group,
			Topic:                 topic,
			WatchPartitionChanges: true,
			MinBytes:              10e3, // 10KB
			MaxBytes:              10e6, // 10MB
			StartOffset:           kafka.LastOffset,
		})

		go k.StartReader(reader)
	}

	return k
}

func (k *KafkaConsumer) StartReader(reader *kafka.Reader) {
	for {
		msg, err := reader.ReadMessage(k.ctx)
		if err != nil {
			fmt.Printf("consumer reading message : %s\n", err.Error())
			if err != nil {
				break
			}
			continue
		}

		receiver, ok := k.Listener[msg.Topic]
		if !ok {
			continue
		}

		go receiver(string(msg.Key), msg.Topic, string(msg.Value))

	}
}
