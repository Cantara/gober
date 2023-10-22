package event

import (
	"fmt"
)

type Type string

const (
	Created Type = "created"
	Updated Type = "updated"
	Deleted Type = "deleted"
	Invalid Type = "invalid"
)

func AllTypes() []Type {
	return []Type{Created, Updated, Deleted}
}

func TypeFromString(s string) Type {
	switch s {
	case string(Created):
		return Created
	case string(Updated):
		return Updated
	case string(Deleted):
		return Deleted
	}
	return Invalid
}

var InvalidTypeError = fmt.Errorf("event type is invalid")
