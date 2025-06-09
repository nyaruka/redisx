package redisx

import (
	"context"
	_ "embed"
	"time"

	"github.com/valkey-io/valkey-go"
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
var isetIsMemberScript = valkey.NewLuaScript(isetIsMember)

// IsMember returns whether we contain the given value
func (s *IntervalSet) IsMember(ctx context.Context, client valkey.Client, member string) (bool, error) {
	keys := s.keys()
	args := []string{member}

	result := isetIsMemberScript.Exec(ctx, client, keys, args)
	if result.Error() != nil {
		return false, result.Error()
	}
	
	count, err := result.ToInt64()
	if err != nil {
		return false, err
	}
	
	return count > 0, nil
}

// Add adds the given value
func (s *IntervalSet) Add(ctx context.Context, client valkey.Client, member string) error {
	key := s.keys()[0]

	// Use pipeline to execute multiple commands atomically
	cmds := []valkey.Completed{
		client.B().Sadd().Key(key).Member(member).Build(),
		client.B().Expire().Key(key).Seconds(int64(s.size * int(s.interval/time.Second))).Build(),
	}
	
	results := client.DoMulti(ctx, cmds...)
	for _, result := range results {
		if result.Error() != nil {
			return result.Error()
		}
	}
	
	return nil
}

// Rem removes the given values
func (s *IntervalSet) Rem(ctx context.Context, client valkey.Client, members ...string) error {
	keys := s.keys()
	
	var cmds []valkey.Completed
	for _, k := range keys {
		cmd := client.B().Srem().Key(k).Member(members...).Build()
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

// Clear removes all values
func (s *IntervalSet) Clear(ctx context.Context, client valkey.Client) error {
	keys := s.keys()
	
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

func (s *IntervalSet) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
