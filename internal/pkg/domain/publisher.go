package domain

import "context"

// Publisher publishes raw messages to a named topic.
// The key is used for partition routing — pass event ID for ordered per-entity delivery.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}
