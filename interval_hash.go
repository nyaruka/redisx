package redisx

import (
	_ "embed"
	"errors"
	"time"
)

// IntervalHash operates like a hash map but with expiring intervals
type IntervalHash struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalHash creates a new empty interval hash
func NewIntervalHash(keyBase string, interval time.Duration, size int) *IntervalHash {
	return &IntervalHash{keyBase: keyBase, interval: interval, size: size}
}

//go:embed lua/ihash_get.lua
var ihashGet string
var ihashGetScript = NewScript(-1, ihashGet)

// Get returns the value of the given field
func (h *IntervalHash) Get(rc Conn, field string) (string, error) {
	keys := h.keys()
	
	// Create args: [len(keys), key1, key2, ..., field]
	args := make([]interface{}, 0, len(keys)+2)
	args = append(args, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, field)

	value, err := String(ihashGetScript.Do(rc, args...))
	if err != nil {
		return "", err
	}
	return value, nil
}

//go:embed lua/ihash_mget.lua
var ihashMGet string
var ihashMGetScript = NewScript(-1, ihashMGet)

// MGet returns the values of the given fields
func (h *IntervalHash) MGet(rc Conn, fields ...string) ([]string, error) {
	keys := h.keys()

	// for consistency with HMGET, zero fields is an error
	if len(fields) == 0 {
		return nil, errors.New("wrong number of arguments for command")
	}

	// Create args: [len(keys), key1, key2, ..., field1, field2, ...]
	args := make([]interface{}, 0, len(keys)+len(fields)+1)
	args = append(args, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	for _, field := range fields {
		args = append(args, field)
	}

	// Note: we ignore ErrNil equivalent in valkey
	return Strings(ihashMGetScript.Do(rc, args...))
}

// Set sets the value of the given field
func (h *IntervalHash) Set(rc Conn, field, value string) error {
	key := h.keys()[0]

	rc.Send("MULTI")
	rc.Send("HSET", key, field, value)
	rc.Send("EXPIRE", key, h.size*int(h.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Del removes the given fields
func (h *IntervalHash) Del(rc Conn, fields ...string) error {
	rc.Send("MULTI")
	for _, k := range h.keys() {
		args := make([]interface{}, 0, len(fields)+1)
		args = append(args, k)
		for _, field := range fields {
			args = append(args, field)
		}
		rc.Send("HDEL", args...)
	}
	_, err := rc.Do("EXEC")
	return err
}

// Clear removes all fields
func (h *IntervalHash) Clear(rc Conn) error {
	rc.Send("MULTI")
	for _, k := range h.keys() {
		rc.Send("DEL", k)
	}
	_, err := rc.Do("EXEC")
	return err
}

func (h *IntervalHash) keys() []string {
	return intervalKeys(h.keyBase, h.interval, h.size)
}
