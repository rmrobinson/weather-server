package hub

import (
	"context"

	"github.com/rmrobinson/weather-server/internal/types"
	"go.uber.org/zap"
)

const subBufferSize = 16

type Subscription struct {
	ID string
	Ch chan types.WeatherReading
}

type Hub struct {
	inbound     chan types.WeatherReading
	subscribe   chan Subscription
	unsubscribe chan string
	done        chan struct{} // closed when Run exits
	logger      *zap.Logger
}

func New(logger *zap.Logger) *Hub {
	return &Hub{
		inbound:     make(chan types.WeatherReading, 16),
		subscribe:   make(chan Subscription),
		unsubscribe: make(chan string),
		done:        make(chan struct{}),
		logger:      logger,
	}
}

// Publish sends a reading into the hub. Non-blocking if the hub is stopped.
func (h *Hub) Publish(r types.WeatherReading) {
	select {
	case h.inbound <- r:
	case <-h.done:
	}
}

// Subscribe registers a new subscriber. Returns immediately (with an unregistered
// channel) if the hub has already stopped.
func (h *Hub) Subscribe(id string) Subscription {
	s := Subscription{
		ID: id,
		Ch: make(chan types.WeatherReading, subBufferSize),
	}
	select {
	case h.subscribe <- s:
	case <-h.done:
	}
	return s
}

// Unsubscribe removes a subscriber by ID. Returns immediately if the hub has stopped.
func (h *Hub) Unsubscribe(id string) {
	select {
	case h.unsubscribe <- id:
	case <-h.done:
	}
}

// Run starts the hub event loop. Call in a goroutine; blocks until ctx is done.
func (h *Hub) Run(ctx context.Context) {
	defer close(h.done)
	subs := make(map[string]chan types.WeatherReading)
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-h.subscribe:
			subs[s.ID] = s.Ch
		case id := <-h.unsubscribe:
			delete(subs, id)
		case r := <-h.inbound:
			for id, ch := range subs {
				select {
				case ch <- r:
				default:
					h.logger.Warn("subscriber channel full, dropping reading", zap.String("subscriber", id))
				}
			}
		}
	}
}
