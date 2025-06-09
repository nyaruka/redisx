package redisx

import (
	_ "embed"
	"time"
)

// IntervalSet operates like a set but with expiring intervals
type IntervalSet struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalSet creates a new empty interval set
func NewIntervalSet(keyBase string, interval time.Duration, size int) *IntervalSet {
	return &IntervalSet{keyBase: keyBase, interval: interval, size: size}
}

//go:embed lua/iset_ismember.lua
var isetIsMember string
var isetIsMemberScript = NewScript(-1, isetIsMember)

// IsMember returns whether we contain the given value
func (s *IntervalSet) IsMember(rc Conn, member string) (bool, error) {
	keys := s.keys()
	
	// Create args: [len(keys), key1, key2, ..., member]
	args := make([]interface{}, 0, len(keys)+2)
	args = append(args, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, member)

	return Bool(isetIsMemberScript.Do(rc, args...))
}

// Add adds the given value
func (s *IntervalSet) Add(rc Conn, member string) error {
	key := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("SADD", key, member)
	rc.Send("EXPIRE", key, s.size*int(s.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Rem removes the given values
func (s *IntervalSet) Rem(rc Conn, members ...string) error {
	rc.Send("MULTI")
	for _, k := range s.keys() {
		args := make([]interface{}, 0, len(members)+1)
		args = append(args, k)
		for _, member := range members {
			args = append(args, member)
		}
		rc.Send("SREM", args...)
	}
	_, err := rc.Do("EXEC")
	return err
}

// Clear removes all values
func (s *IntervalSet) Clear(rc Conn) error {
	rc.Send("MULTI")
	for _, k := range s.keys() {
		rc.Send("DEL", k)
	}
	_, err := rc.Do("EXEC")
	return err
}

func (s *IntervalSet) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
