package messaging

import (
	"context"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		CommitInterval: 0, // manual commit
	})
	return &Consumer{reader: r}
}

func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

// Lag returns the current consumer lag (messages behind latest offset).
func (c *Consumer) Lag() int64 {
	return c.reader.Stats().Lag
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
