package redisx

import (
	_ "embed"
	"strconv"
	"time"
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
func (s *IntervalSeries) Record(rc Conn, field string, value int64) error {
	currKey := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("HINCRBY", currKey, field, value)
	rc.Send("EXPIRE", currKey, s.size*int(s.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

//go:embed lua/iseries_get.lua
var iseriesGet string
var iseriesGetScript = NewScript(-1, iseriesGet)

// Get gets the values of field in all intervals
func (s *IntervalSeries) Get(rc Conn, field string) ([]int64, error) {
	keys := s.keys()
	
	// Create args: [len(keys), key1, key2, ..., field]
	args := make([]interface{}, 0, len(keys)+2)
	args = append(args, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, field)

	values, err := Strings(iseriesGetScript.Do(rc, args...))
	if err != nil {
		return nil, err
	}

	result := make([]int64, len(values))
	for i, v := range values {
		if v == "" {
			result[i] = 0
		} else {
			result[i], err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// Total gets the total value of field across all intervals
func (s *IntervalSeries) Total(rc Conn, field string) (int64, error) {
	vals, err := s.Get(rc, field)
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
