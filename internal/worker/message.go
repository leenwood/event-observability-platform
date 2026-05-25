package worker

import (
	"encoding/json"
	"fmt"
)

// EventMessage is the payload written to the events Kafka topic.
type EventMessage struct {
	EventID string `json:"event_id"`
	Attempt int    `json:"attempt"`
}

func MarshalEventMessage(msg EventMessage) ([]byte, error) {
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal event message: %w", err)
	}
	return b, nil
}

func UnmarshalEventMessage(data []byte) (EventMessage, error) {
	var msg EventMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return EventMessage{}, fmt.Errorf("unmarshal event message: %w", err)
	}
	return msg, nil
}
