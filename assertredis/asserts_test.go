package assertredis_test

import (
	"testing"

	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
)

func TestAsserts(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	rc.Do("SET", "mykey", "one")

	assert.True(t, assertredis.Exists(t, rp, "mykey"))
	assert.True(t, assertredis.NotExists(t, rp, "mykey2"))
	assert.True(t, assertredis.Get(t, rp, "mykey", "one"))

	rc.Do("RPUSH", "mylist", "one")
	rc.Do("RPUSH", "mylist", "two")
	rc.Do("RPUSH", "mylist", "three")

	assert.True(t, assertredis.LLen(t, rp, "mylist", 3))
	assert.True(t, assertredis.LRange(t, rp, "mylist", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.LGetAll(t, rp, "mylist", []string{"one", "two", "three"}))

	rc.Do("SADD", "myset", "one")
	rc.Do("SADD", "myset", "two")
	rc.Do("SADD", "myset", "three")

	assert.True(t, assertredis.SCard(t, rp, "myset", 3))
	assert.True(t, assertredis.SIsMember(t, rp, "myset", "two"))
	assert.True(t, assertredis.SIsNotMember(t, rp, "myset", "four"))
	assert.True(t, assertredis.SMembers(t, rp, "myset", []string{"two", "one", "three"}))

	rc.Do("HSET", "myhash", "a", "one")
	rc.Do("HSET", "myhash", "b", "two")
	rc.Do("HSET", "myhash", "c", "three")

	assert.True(t, assertredis.HLen(t, rp, "myhash", 3))
	assert.True(t, assertredis.HGet(t, rp, "myhash", "b", "two"))
	assert.True(t, assertredis.HGetAll(t, rp, "myhash", map[string]string{"a": "one", "b": "two", "c": "three"}))

	rc.Do("ZADD", "myzset", 1, "one")
	rc.Do("ZADD", "myzset", 2, "two")
	rc.Do("ZADD", "myzset", 3, "three")

	assert.True(t, assertredis.ZCard(t, rp, "myzset", 3))
	assert.True(t, assertredis.ZScore(t, rp, "myzset", "one", 1))
	assert.True(t, assertredis.ZScore(t, rp, "myzset", "two", 2))
	assert.True(t, assertredis.ZScore(t, rp, "myzset", "three", 3))
	assert.True(t, assertredis.ZRange(t, rp, "myzset", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.ZGetAll(t, rp, "myzset", map[string]float64{"one": 1, "two": 2, "three": 3}))
}
