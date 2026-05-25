package dto

// EventMessage is the payload written to the events Kafka topic.
type EventMessage struct {
	EventID string `json:"event_id"`
	Attempt int    `json:"attempt"`
}
