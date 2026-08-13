package notification

import (
	"sync"
	"time"
)

const HistoryLimit = 64

type Message struct {
	ID       uint64         `json:"id"`
	Time     time.Time      `json:"time"`
	Title    string         `json:"title"`
	Subtitle string         `json:"subtitle,omitempty"`
	Body     string         `json:"body,omitempty"`
	Options  map[string]any `json:"options,omitempty"`
	Script   string         `json:"script,omitempty"`
}

type Snapshot struct {
	Limit         int       `json:"limit"`
	Notifications []Message `json:"notifications"`
}

type Event struct {
	Type          string    `json:"type"`
	Limit         int       `json:"limit,omitempty"`
	Notifications []Message `json:"notifications,omitempty"`
	Notification  *Message  `json:"notification,omitempty"`
}

type broker struct {
	mutex       sync.Mutex
	nextID      uint64
	history     []Message
	subscribers map[chan Event]struct{}
}

var defaultBroker = &broker{subscribers: make(map[chan Event]struct{})}

func Post(title, subtitle, body, script string, options map[string]any) Message {
	return defaultBroker.post(Message{
		Title:    title,
		Subtitle: subtitle,
		Body:     body,
		Options:  cloneOptions(options),
		Script:   script,
	})
}

func NotificationsSnapshot() Snapshot {
	return defaultBroker.snapshot()
}

func Subscribe() (<-chan Event, func()) {
	return defaultBroker.subscribe()
}

func Clear() {
	defaultBroker.clear()
}

func (b *broker) post(message Message) Message {
	b.mutex.Lock()
	b.nextID++
	message.ID = b.nextID
	message.Time = time.Now()
	b.history = append(b.history, message)
	if len(b.history) > HistoryLimit {
		b.history = append([]Message(nil), b.history[len(b.history)-HistoryLimit:]...)
	}

	eventMessage := cloneMessage(message)
	event := Event{Type: "notification", Notification: &eventMessage}
	for subscriber := range b.subscribers {
		select {
		case subscriber <- event:
		default:
			delete(b.subscribers, subscriber)
			close(subscriber)
		}
	}
	b.mutex.Unlock()
	return cloneMessage(message)
}

func (b *broker) snapshot() Snapshot {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	notifications := make([]Message, len(b.history))
	for index := range b.history {
		notifications[index] = cloneMessage(b.history[index])
	}
	return Snapshot{Limit: HistoryLimit, Notifications: notifications}
}

func (b *broker) subscribe() (<-chan Event, func()) {
	subscriber := make(chan Event, HistoryLimit)
	b.mutex.Lock()
	b.subscribers[subscriber] = struct{}{}
	b.mutex.Unlock()

	var once sync.Once
	return subscriber, func() {
		once.Do(func() {
			b.mutex.Lock()
			if _, found := b.subscribers[subscriber]; found {
				delete(b.subscribers, subscriber)
				close(subscriber)
			}
			b.mutex.Unlock()
		})
	}
}

func (b *broker) clear() {
	b.mutex.Lock()
	b.history = nil
	b.mutex.Unlock()
}

func cloneMessage(message Message) Message {
	message.Options = cloneOptions(message.Options)
	return message
}

func cloneOptions(options map[string]any) map[string]any {
	if len(options) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(options))
	for key, value := range options {
		cloned[key] = value
	}
	return cloned
}
