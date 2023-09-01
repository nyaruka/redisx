package redisx

import (
	_ "embed"
	"time"

	"github.com/gomodule/redigo/redis"
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
var isetIsMemberScript = redis.NewScript(-1, isetIsMember)

// IsMember returns whether we contain the given value
func (s *IntervalSet) IsMember(rc redis.Conn, member string) (bool, error) {
	keys := s.keys()

	return redis.Bool(isetIsMemberScript.Do(rc, redis.Args{}.Add(len(keys)).AddFlat(keys).Add(member)...))
}

// Add adds the given value
func (s *IntervalSet) Add(rc redis.Conn, member string) error {
	key := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("SADD", key, member)
	rc.Send("EXPIRE", key, s.size*int(s.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Rem removes the given value
func (s *IntervalSet) Rem(rc redis.Conn, member string) error {
	rc.Send("MULTI")
	for _, k := range s.keys() {
		rc.Send("SREM", k, member)
	}
	_, err := rc.Do("EXEC")
	return err
}

// Clear removes all values
func (s *IntervalSet) Clear(rc redis.Conn) error {
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
