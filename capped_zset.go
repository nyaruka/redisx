package redisx

import (
	_ "embed"
	"time"
)

// CappedZSet is a sorted set but enforces a cap on size
type CappedZSet struct {
	key    string
	cap    int
	expire time.Duration
}

// NewCappedZSet creates a new capped sorted set
func NewCappedZSet(key string, cap int, expire time.Duration) *CappedZSet {
	return &CappedZSet{key: key, cap: cap, expire: expire}
}

//go:embed lua/czset_add.lua
var czsetAdd string
var czsetAddScript = NewScript(1, czsetAdd)

// Add adds an element to the set, if its score puts in the top `cap` members
func (z *CappedZSet) Add(rc Conn, member string, score float64) error {
	_, err := czsetAddScript.Do(rc, z.key, score, member, z.cap, int(z.expire/time.Second))
	return err
}

// Card returns the cardinality of the set
func (z *CappedZSet) Card(rc Conn) (int, error) {
	return Int(rc.Do("ZCARD", z.key))
}

// Members returns all members of the set, ordered by ascending rank
func (z *CappedZSet) Members(rc Conn) ([]string, []float64, error) {
	return StringsWithScores(rc.Do("ZRANGE", z.key, 0, -1, "WITHSCORES"))
}
