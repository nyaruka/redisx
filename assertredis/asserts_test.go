package assertredis_test

import (
	"context"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
)

func TestAsserts(t *testing.T) {
	ctx := context.Background()
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	redis.DoContext(rc, ctx, "SET", "mykey", "one")

	assert.True(t, assertredis.Exists(t, rc, "mykey"))
	assert.True(t, assertredis.NotExists(t, rc, "mykey2"))
	assert.True(t, assertredis.Get(t, rc, "mykey", "one"))

	redis.DoContext(rc, ctx, "RPUSH", "mylist", "one")
	redis.DoContext(rc, ctx, "RPUSH", "mylist", "two")
	redis.DoContext(rc, ctx, "RPUSH", "mylist", "three")

	assert.True(t, assertredis.LLen(t, rc, "mylist", 3))
	assert.True(t, assertredis.LRange(t, rc, "mylist", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.LGetAll(t, rc, "mylist", []string{"one", "two", "three"}))

	redis.DoContext(rc, ctx, "SADD", "myset", "one")
	redis.DoContext(rc, ctx, "SADD", "myset", "two")
	redis.DoContext(rc, ctx, "SADD", "myset", "three")

	assert.True(t, assertredis.SCard(t, rc, "myset", 3))
	assert.True(t, assertredis.SIsMember(t, rc, "myset", "two"))
	assert.True(t, assertredis.SIsNotMember(t, rc, "myset", "four"))
	assert.True(t, assertredis.SMembers(t, rc, "myset", []string{"two", "one", "three"}))

	redis.DoContext(rc, ctx, "HSET", "myhash", "a", "one")
	redis.DoContext(rc, ctx, "HSET", "myhash", "b", "two")
	redis.DoContext(rc, ctx, "HSET", "myhash", "c", "three")

	assert.True(t, assertredis.HLen(t, rc, "myhash", 3))
	assert.True(t, assertredis.HGet(t, rc, "myhash", "b", "two"))
	assert.True(t, assertredis.HGetAll(t, rc, "myhash", map[string]string{"a": "one", "b": "two", "c": "three"}))

	redis.DoContext(rc, ctx, "ZADD", "myzset", 1, "one")
	redis.DoContext(rc, ctx, "ZADD", "myzset", 2, "two")
	redis.DoContext(rc, ctx, "ZADD", "myzset", 3, "three")

	assert.True(t, assertredis.ZCard(t, rc, "myzset", 3))
	assert.True(t, assertredis.ZScore(t, rc, "myzset", "one", 1))
	assert.True(t, assertredis.ZScore(t, rc, "myzset", "two", 2))
	assert.True(t, assertredis.ZScore(t, rc, "myzset", "three", 3))
	assert.True(t, assertredis.ZRange(t, rc, "myzset", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.ZGetAll(t, rc, "myzset", map[string]float64{"one": 1, "two": 2, "three": 3}))
}
