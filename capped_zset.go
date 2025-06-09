package redisx

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
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
var czsetAddScript = valkey.NewLuaScript(czsetAdd)

// Add adds an element to the set, if its score puts in the top `cap` members
func (z *CappedZSet) Add(ctx context.Context, client valkey.Client, member string, score float64) error {
	keys := []string{z.key}
	args := []string{fmt.Sprintf("%f", score), member, fmt.Sprintf("%d", z.cap), fmt.Sprintf("%d", int(z.expire/time.Second))}
	
	result := czsetAddScript.Exec(ctx, client, keys, args)
	return result.Error()
}

// Card returns the cardinality of the set
func (z *CappedZSet) Card(ctx context.Context, client valkey.Client) (int, error) {
	cmd := client.B().Zcard().Key(z.key).Build()
	result := client.Do(ctx, cmd)
	
	if result.Error() != nil {
		return 0, result.Error()
	}
	
	count, err := result.ToInt64()
	return int(count), err
}

// Members returns all members of the set, ordered by ascending rank
func (z *CappedZSet) Members(ctx context.Context, client valkey.Client) ([]string, []float64, error) {
	cmd := client.B().Zrange().Key(z.key).Min("0").Max("-1").Withscores().Build()
	result := client.Do(ctx, cmd)
	
	if result.Error() != nil {
		return nil, nil, result.Error()
	}
	
	return StringsWithScores(result, nil)
}
