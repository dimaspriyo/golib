package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"runtime"
	"strings"
)

type HandlerListener func(ctx context.Context, reader *kafka.Reader, msg kafka.Message)

type KafkaConsumer struct {
	conn     *kafka.Conn
	Listener map[string]HandlerListener
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

		fmt.Println("Add Listener For Topic: ", topic)

		go k.StartReader(reader)
	}

	return k
}

func (k *KafkaConsumer) StartReader(reader *kafka.Reader) {
	for {
		msg, err := reader.FetchMessage(k.ctx)
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

		fmt.Println("Add goroutine for topic: ", msg.Topic)
		fmt.Println("Current goroutine: ", runtime.NumGoroutine())

		go receiver(k.ctx, reader, msg)

	}
}

func (k *KafkaConsumer) SetListener(topic string, listener HandlerListener) {
	if k.Listener == nil {
		k.Listener = make(map[string]HandlerListener, 0)
	}

	k.Listener[topic] = listener
}
