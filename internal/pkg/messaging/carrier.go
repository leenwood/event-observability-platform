package messaging

import "github.com/segmentio/kafka-go"

// HeaderCarrier adapts []kafka.Header to the OTel TextMapCarrier interface,
// enabling trace context injection on produce and extraction on consume.
type HeaderCarrier struct {
	headers []kafka.Header
}

func NewHeaderCarrier(h []kafka.Header) *HeaderCarrier {
	return &HeaderCarrier{headers: append([]kafka.Header(nil), h...)}
}

func (c *HeaderCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *HeaderCarrier) Set(key, value string) {
	for i, h := range c.headers {
		if h.Key == key {
			c.headers[i].Value = []byte(value)
			return
		}
	}
	c.headers = append(c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c *HeaderCarrier) Keys() []string {
	keys := make([]string, len(c.headers))
	for i, h := range c.headers {
		keys[i] = h.Key
	}
	return keys
}

func (c *HeaderCarrier) Headers() []kafka.Header {
	return c.headers
}
