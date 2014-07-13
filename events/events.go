package events

import (
	"sync"
	"time"
)

type EventType uint64

const (
	NodeConnected = 1 << iota
	NodeDisconnected
	LocalIndexUpdated
	RemoteIndexUpdated
	ItemStarted
	ItemCompleted

	AllEvents = NodeConnected | NodeDisconnected | LocalIndexUpdated | RemoteIndexUpdated | ItemStarted | ItemCompleted
)

const BufferSize = 64

type Logger struct {
	subs   map[int]*Subscription
	nextId int
	mutex  sync.Mutex
}

type Event struct {
	ID   int
	Type EventType
	Meta interface{}
}

type Subscription struct {
	mask   EventType
	id     int
	events chan Event
	timer  *time.Timer
	mutex  sync.Mutex
}

var Default = NewLogger()

func (s *Subscription) Poll(timeout time.Duration) (Event, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.timer.Reset(timeout)
	select {
	case e, ok := <-s.events:
		return e, ok
	case <-s.timer.C:
		return Event{}, false
	}
}

func NewLogger() *Logger {
	return &Logger{
		subs: make(map[int]*Subscription),
	}
}

func (l *Logger) Log(t EventType, meta interface{}) {
	l.mutex.Lock()
	e := Event{
		ID:   l.nextId,
		Type: t,
		Meta: meta,
	}
	l.nextId++
	for _, s := range l.subs {
		if s.mask&t != 0 {
			select {
			case s.events <- e:
			default:
			}
		}
	}
	l.mutex.Unlock()
}

func (l *Logger) Subscribe(mask EventType) *Subscription {
	l.mutex.Lock()
	s := &Subscription{
		mask:   mask,
		id:     l.nextId,
		events: make(chan Event, BufferSize),
		timer:  time.NewTimer(0),
	}
	l.nextId++
	l.subs[s.id] = s
	l.mutex.Unlock()
	return s
}

func (l *Logger) Unsubscribe(s *Subscription) {
	l.mutex.Lock()
	delete(l.subs, s.id)
	close(s.events)
	l.mutex.Unlock()
}
