package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/leenwood/event-observability-platform/internal/pkg/outbound/httpclient"
)

// Notifier sends event notifications to an external webhook endpoint.
// It demonstrates using the resilient HTTP client with retry and circuit breaker.
type Notifier struct {
	client *httpclient.Client
}

func New(client *httpclient.Client) *Notifier {
	return &Notifier{client: client}
}

type eventNotification struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	Source    string `json:"source"`
}

// Notify posts an event notification to the configured external endpoint.
func (n *Notifier) Notify(ctx context.Context, eventID, eventType, source string) error {
	payload, err := json.Marshal(eventNotification{
		EventID:   eventID,
		EventType: eventType,
		Source:    source,
	})
	if err != nil {
		return fmt.Errorf("notifier: marshal payload: %w", err)
	}

	resp, err := n.client.Do(ctx, http.MethodPost, "/notify", payload)
	if err != nil {
		return fmt.Errorf("notifier: send notification: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("notifier: unexpected status %d: %s", resp.StatusCode, resp.Body)
	}

	return nil
}
