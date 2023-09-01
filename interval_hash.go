package redisx

import (
	_ "embed"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
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
var ihashGetScript = redis.NewScript(-1, ihashGet)

// Get returns the value of the given field
func (h *IntervalHash) Get(rc redis.Conn, field string) (string, error) {
	keys := h.keys()

	value, err := redis.String(ihashGetScript.Do(rc, redis.Args{}.Add(len(keys)).AddFlat(keys).Add(field)...))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return value, nil
}

//go:embed lua/ihash_mget.lua
var ihashMGet string
var ihashMGetScript = redis.NewScript(-1, ihashMGet)

// MGet returns the values of the given fields
func (h *IntervalHash) MGet(rc redis.Conn, fields ...string) ([]string, error) {
	keys := h.keys()

	// for consistency with HMGET, zero fields is an error
	if len(fields) == 0 {
		return nil, errors.New("wrong number of arguments for command")
	}

	value, err := redis.Strings(ihashMGetScript.Do(rc, redis.Args{}.Add(len(keys)).AddFlat(keys).AddFlat(fields)...))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}
	return value, nil
}

// Set sets the value of the given field
func (h *IntervalHash) Set(rc redis.Conn, field, value string) error {
	key := h.keys()[0]

	rc.Send("MULTI")
	rc.Send("HSET", key, field, value)
	rc.Send("EXPIRE", key, h.size*int(h.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Del removes the given field
func (h *IntervalHash) Del(rc redis.Conn, field string) error {
	rc.Send("MULTI")
	for _, k := range h.keys() {
		rc.Send("HDEL", k, field)
	}
	_, err := rc.Do("EXEC")
	return err
}

// Clear removes all fields
func (h *IntervalHash) Clear(rc redis.Conn) error {
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
