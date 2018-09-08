package file

import (
	"fmt"
	"time"
)

type EventType uint8

const (
	Create EventType = iota
	Update
	Delete
)

type Event struct {
	Type      EventType
	Timestamp time.Time
}

func New(et EventType) *Event {
	return &Event{Type: et, Timestamp: time.Now()}
}

func (et *Event) String() string {
	var typeStr string
	switch et.Type {
	case Create:
		typeStr = "Create"
	case Update:
		typeStr = "Update"
	case Delete:
		typeStr = "Delete"
	default:
		typeStr = "Undefined"
	}
	return fmt.Sprintf("[Event: %s, Time: %s]", typeStr, et.Timestamp)
}
