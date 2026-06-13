package hub

import (
	"testing"
	"time"

	"github.com/rmrobinson/weather-server/internal/types"
	"go.uber.org/zap"
)

func TestHub(t *testing.T) {
	logger := zap.NewNop()
	h := New(logger)

	go h.Run(t.Context())

	s1 := h.Subscribe("sub1")
	s2 := h.Subscribe("sub2")

	reading := types.WeatherReading{Timestamp: time.Now(), TempC: 21.5}
	h.Publish(reading)

	for _, sub := range []Subscription{s1, s2} {
		select {
		case got := <-sub.Ch:
			if got.TempC != reading.TempC {
				t.Errorf("subscriber %s: got TempC %v, want %v", sub.ID, got.TempC, reading.TempC)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %s: timed out waiting for reading", sub.ID)
		}
	}

	// Unsubscribe sends on an unbuffered channel, so it returns only after the
	// hub's event loop has received and processed the removal. No sleep needed.
	h.Unsubscribe("sub1")

	reading2 := types.WeatherReading{Timestamp: time.Now(), TempC: 22.0}
	h.Publish(reading2)

	select {
	case got := <-s2.Ch:
		if got.TempC != reading2.TempC {
			t.Errorf("sub2: got TempC %v, want %v", got.TempC, reading2.TempC)
		}
	case <-time.After(time.Second):
		t.Error("sub2: timed out waiting for second reading")
	}

	select {
	case r := <-s1.Ch:
		t.Errorf("sub1: unexpectedly received reading after unsubscribe: %v", r)
	case <-time.After(50 * time.Millisecond):
		// expected: nothing received
	}
}
