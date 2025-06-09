package redisx

import (
	"context"
	_ "embed"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
)

// IntervalSeries returns all values from interval based hashes.
type IntervalSeries struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalSeries creates a new empty series
func NewIntervalSeries(keyBase string, interval time.Duration, size int) *IntervalSeries {
	return &IntervalSeries{keyBase: keyBase, interval: interval, size: size}
}

// Record increments the value of field by value in the current interval
func (s *IntervalSeries) Record(ctx context.Context, client valkey.Client, field string, value int64) error {
	currKey := s.keys()[0]

	// Use pipeline to execute multiple commands atomically
	cmds := []valkey.Completed{
		client.B().Hincrby().Key(currKey).Field(field).Increment(value).Build(),
		client.B().Expire().Key(currKey).Seconds(int64(s.size * int(s.interval/time.Second))).Build(),
	}
	
	results := client.DoMulti(ctx, cmds...)
	for _, result := range results {
		if result.Error() != nil {
			return result.Error()
		}
	}
	
	return nil
}

//go:embed lua/iseries_get.lua
var iseriesGet string
var iseriesGetScript = valkey.NewLuaScript(iseriesGet)

// Get gets the values of field in all intervals
func (s *IntervalSeries) Get(ctx context.Context, client valkey.Client, field string) ([]int64, error) {
	keys := s.keys()
	args := []string{field}

	result := iseriesGetScript.Exec(ctx, client, keys, args)
	if result.Error() != nil {
		return nil, result.Error()
	}
	
	// Convert array to strings
	arr, err := result.ToArray()
	if err != nil {
		return nil, err
	}
	
	values := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		if err != nil {
			return nil, err
		}
		values[i] = str
	}

	resultInts := make([]int64, len(values))
	for i, v := range values {
		if v == "" {
			resultInts[i] = 0
		} else {
			resultInts[i], err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}

	return resultInts, nil
}

// Total gets the total value of field across all intervals
func (s *IntervalSeries) Total(ctx context.Context, client valkey.Client, field string) (int64, error) {
	vals, err := s.Get(ctx, client, field)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, v := range vals {
		total += v
	}
	return total, nil
}

func (s *IntervalSeries) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
