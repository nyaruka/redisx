package redisx

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/valkey-io/valkey-go"
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
var ihashGetScript = valkey.NewLuaScript(ihashGet)

// Get returns the value of the given field
func (h *IntervalHash) Get(ctx context.Context, client valkey.Client, field string) (string, error) {
	keys := h.keys()
	args := []string{field}

	result := ihashGetScript.Exec(ctx, client, keys, args)
	if result.Error() != nil {
		return "", result.Error()
	}
	
	return result.ToString()
}

//go:embed lua/ihash_mget.lua
var ihashMGet string
var ihashMGetScript = valkey.NewLuaScript(ihashMGet)

// MGet returns the values of the given fields
func (h *IntervalHash) MGet(ctx context.Context, client valkey.Client, fields ...string) ([]string, error) {
	keys := h.keys()

	// for consistency with HMGET, zero fields is an error
	if len(fields) == 0 {
		return nil, errors.New("wrong number of arguments for command")
	}

	result := ihashMGetScript.Exec(ctx, client, keys, fields)
	if result.Error() != nil {
		return nil, result.Error()
	}
	
	// Convert array to strings
	arr, err := result.ToArray()
	if err != nil {
		return nil, err
	}
	
	strings := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		if err != nil {
			return nil, err
		}
		strings[i] = str
	}
	
	return strings, nil
}

// Set sets the value of the given field
func (h *IntervalHash) Set(ctx context.Context, client valkey.Client, field, value string) error {
	key := h.keys()[0]
	
	// Use pipeline to execute multiple commands atomically
	cmds := []valkey.Completed{
		client.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build(),
		client.B().Expire().Key(key).Seconds(int64(h.size * int(h.interval/time.Second))).Build(),
	}
	
	results := client.DoMulti(ctx, cmds...)
	for _, result := range results {
		if result.Error() != nil {
			return result.Error()
		}
	}
	
	return nil
}

// Del removes the given fields
func (h *IntervalHash) Del(ctx context.Context, client valkey.Client, fields ...string) error {
	keys := h.keys()
	
	var cmds []valkey.Completed
	for _, k := range keys {
		cmd := client.B().Hdel().Key(k).Field(fields...).Build()
		cmds = append(cmds, cmd)
	}
	
	results := client.DoMulti(ctx, cmds...)
	for _, result := range results {
		if result.Error() != nil {
			return result.Error()
		}
	}
	
	return nil
}

// Clear removes all fields
func (h *IntervalHash) Clear(ctx context.Context, client valkey.Client) error {
	keys := h.keys()
	
	var cmds []valkey.Completed
	for _, k := range keys {
		cmd := client.B().Del().Key(k).Build()
		cmds = append(cmds, cmd)
	}
	
	results := client.DoMulti(ctx, cmds...)
	for _, result := range results {
		if result.Error() != nil {
			return result.Error()
		}
	}
	
	return nil
}

func (h *IntervalHash) keys() []string {
	return intervalKeys(h.keyBase, h.interval, h.size)
}
