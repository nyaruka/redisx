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

	assert.True(t, assertredis.Exists(t, rc, "mykey"))
	assert.True(t, assertredis.NotExists(t, rc, "mykey2"))
	assert.True(t, assertredis.Get(t, rc, "mykey", "one"))

	rc.Do("RPUSH", "mylist", "one")
	rc.Do("RPUSH", "mylist", "two")
	rc.Do("RPUSH", "mylist", "three")

	assert.True(t, assertredis.LLen(t, rc, "mylist", 3))
	assert.True(t, assertredis.LRange(t, rc, "mylist", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.LGetAll(t, rc, "mylist", []string{"one", "two", "three"}))

	rc.Do("SADD", "myset", "one")
	rc.Do("SADD", "myset", "two")
	rc.Do("SADD", "myset", "three")

	assert.True(t, assertredis.SCard(t, rc, "myset", 3))
	assert.True(t, assertredis.SIsMember(t, rc, "myset", "two"))
	assert.True(t, assertredis.SIsNotMember(t, rc, "myset", "four"))
	assert.True(t, assertredis.SMembers(t, rc, "myset", []string{"two", "one", "three"}))

	rc.Do("HSET", "myhash", "a", "one")
	rc.Do("HSET", "myhash", "b", "two")
	rc.Do("HSET", "myhash", "c", "three")

	assert.True(t, assertredis.HLen(t, rc, "myhash", 3))
	assert.True(t, assertredis.HGet(t, rc, "myhash", "b", "two"))
	assert.True(t, assertredis.HGetAll(t, rc, "myhash", map[string]string{"a": "one", "b": "two", "c": "three"}))

	rc.Do("ZADD", "myzset", 1, "one")
	rc.Do("ZADD", "myzset", 2, "two")
	rc.Do("ZADD", "myzset", 3, "three")

	assert.True(t, assertredis.ZCard(t, rc, "myzset", 3))
	assert.True(t, assertredis.ZScore(t, rc, "myzset", "one", 1))
	assert.True(t, assertredis.ZScore(t, rc, "myzset", "two", 2))
	assert.True(t, assertredis.ZScore(t, rc, "myzset", "three", 3))
	assert.True(t, assertredis.ZRange(t, rc, "myzset", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.ZGetAll(t, rc, "myzset", map[string]float64{"one": 1, "two": 2, "three": 3}))
}
