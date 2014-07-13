package events_test

import (
	"testing"
	"time"

	"github.com/calmh/syncthing/events"
)

func TestNewLogger(t *testing.T) {
	l := events.NewLogger()
	if l == nil {
		t.Fatal("Unexpected nil Logger")
	}
}

func TestSubscriber(t *testing.T) {
	l := events.NewLogger()
	s := l.Subscribe(0)
	if s == nil {
		t.Fatal("Unexpected nil Subscription")
	}
}

func TestTimeout(t *testing.T) {
	l := events.NewLogger()
	s := l.Subscribe(0)
	ev, ok := s.Poll(100 * time.Millisecond)
	if ok {
		t.Fatal("Unexpected OK poll, event:", ev)
	}
}

func TestEventBeforeSubscribe(t *testing.T) {
	l := events.NewLogger()

	l.Log(events.NodeConnected, "foo")
	s := l.Subscribe(0)

	ev, ok := s.Poll(100 * time.Millisecond)

	if ok {
		t.Fatal("Unexpected OK poll, event:", ev)
	}
}

func TestEventAfterSubscribe(t *testing.T) {
	l := events.NewLogger()

	s := l.Subscribe(events.AllEvents)
	l.Log(events.NodeConnected, "foo")

	ev, ok := s.Poll(100 * time.Millisecond)

	if !ok {
		t.Fatal("Unexpected failed poll")
	}
	if ev.Type != events.NodeConnected {
		t.Error("Incorrect event type", ev.Type)
	}
	switch v := ev.Meta.(type) {
	case string:
		if v != "foo" {
			t.Error("Incorrect meta string", v)
		}
	default:
		t.Errorf("Incorrect meta type %#v", v)
	}
}

func TestEventAfterSubscribeIgnoreMask(t *testing.T) {
	l := events.NewLogger()

	s := l.Subscribe(events.NodeDisconnected)
	l.Log(events.NodeConnected, "foo")

	ev, ok := s.Poll(100 * time.Millisecond)

	if ok {
		t.Fatal("Unexpected OK poll, event:", ev)
	}
}

func TestBufferOverflow(t *testing.T) {
	l := events.NewLogger()

	_ = l.Subscribe(events.AllEvents)

	t0 := time.Now()
	for i := 0; i < events.BufferSize*2; i++ {
		l.Log(events.NodeConnected, "foo")
	}
	if time.Since(t0) > 100*time.Millisecond {
		t.Fatalf("Logging took too long")
	}
}

func TestUnsubscribe(t *testing.T) {
	l := events.NewLogger()

	s := l.Subscribe(events.AllEvents)
	l.Log(events.NodeConnected, "foo")

	_, ok := s.Poll(100 * time.Millisecond)
	if !ok {
		t.Fatal("Unexpected failed poll")
	}

	l.Unsubscribe(s)
	l.Log(events.NodeConnected, "foo")

	ev, ok := s.Poll(100 * time.Millisecond)
	if ok {
		t.Fatal("Unexpected OK poll, event:", ev)
	}
}

func TestIDs(t *testing.T) {
	l := events.NewLogger()

	s := l.Subscribe(events.AllEvents)
	l.Log(events.NodeConnected, "foo")
	l.Log(events.NodeConnected, "bar")

	ev, ok := s.Poll(100 * time.Millisecond)
	if !ok {
		t.Fatal("Unexpected failed poll")
	}
	if ev.Meta.(string) != "foo" {
		t.Fatal("Incorrect event:", ev)
	}
	id := ev.ID

	ev, ok = s.Poll(100 * time.Millisecond)
	if !ok {
		t.Fatal("Unexpected failed poll")
	}
	if ev.Meta.(string) != "bar" {
		t.Fatal("Incorrect event:", ev)
	}
	if !(ev.ID > id) {
		t.Fatalf("ID not incremented (%d !> %d)", ev.ID, id)
	}
}
