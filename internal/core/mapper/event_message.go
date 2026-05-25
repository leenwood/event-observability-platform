package mapper

import (
	"encoding/json"
	"fmt"

	"github.com/leenwood/event-observability-platform/internal/core/dto"
)

func MarshalEventMessage(msg dto.EventMessage) ([]byte, error) {
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal event message: %w", err)
	}
	return b, nil
}

func UnmarshalEventMessage(data []byte) (dto.EventMessage, error) {
	var msg dto.EventMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return dto.EventMessage{}, fmt.Errorf("unmarshal event message: %w", err)
	}
	return msg, nil
}
