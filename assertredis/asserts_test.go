package assertredis_test

import (
	"context"
	"testing"

	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
)

func TestAsserts(t *testing.T) {
	client := assertredis.TestDB()
	defer client.Close()

	defer assertredis.FlushDB()

	ctx := context.Background()
	client.Do(ctx, client.B().Set().Key("mykey").Value("one").Build())

	assert.True(t, assertredis.Exists(t, client, "mykey"))
	assert.True(t, assertredis.NotExists(t, client, "mykey2"))
	assert.True(t, assertredis.Get(t, client, "mykey", "one"))

	client.Do(ctx, client.B().Rpush().Key("mylist").Element("one").Build())
	client.Do(ctx, client.B().Rpush().Key("mylist").Element("two").Build())
	client.Do(ctx, client.B().Rpush().Key("mylist").Element("three").Build())

	assert.True(t, assertredis.LLen(t, client, "mylist", 3))
	assert.True(t, assertredis.LRange(t, client, "mylist", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.LGetAll(t, client, "mylist", []string{"one", "two", "three"}))

	client.Do(ctx, client.B().Sadd().Key("myset").Member("one").Build())
	client.Do(ctx, client.B().Sadd().Key("myset").Member("two").Build())
	client.Do(ctx, client.B().Sadd().Key("myset").Member("three").Build())

	assert.True(t, assertredis.SCard(t, client, "myset", 3))
	assert.True(t, assertredis.SIsMember(t, client, "myset", "two"))
	assert.True(t, assertredis.SIsNotMember(t, client, "myset", "four"))
	assert.True(t, assertredis.SMembers(t, client, "myset", []string{"two", "one", "three"}))

	client.Do(ctx, client.B().Hset().Key("myhash").FieldValue().FieldValue("a", "one").Build())
	client.Do(ctx, client.B().Hset().Key("myhash").FieldValue().FieldValue("b", "two").Build())
	client.Do(ctx, client.B().Hset().Key("myhash").FieldValue().FieldValue("c", "three").Build())

	assert.True(t, assertredis.HLen(t, client, "myhash", 3))
	assert.True(t, assertredis.HGet(t, client, "myhash", "b", "two"))
	assert.True(t, assertredis.HGetAll(t, client, "myhash", map[string]string{"a": "one", "b": "two", "c": "three"}))

	client.Do(ctx, client.B().Zadd().Key("myzset").ScoreMember().ScoreMember(1, "one").Build())
	client.Do(ctx, client.B().Zadd().Key("myzset").ScoreMember().ScoreMember(2, "two").Build())
	client.Do(ctx, client.B().Zadd().Key("myzset").ScoreMember().ScoreMember(3, "three").Build())

	assert.True(t, assertredis.ZCard(t, client, "myzset", 3))
	assert.True(t, assertredis.ZScore(t, client, "myzset", "one", 1))
	assert.True(t, assertredis.ZScore(t, client, "myzset", "two", 2))
	assert.True(t, assertredis.ZScore(t, client, "myzset", "three", 3))
	assert.True(t, assertredis.ZRange(t, client, "myzset", 0, 1, []string{"one", "two"}))
	assert.True(t, assertredis.ZGetAll(t, client, "myzset", map[string]float64{"one": 1, "two": 2, "three": 3}))
}
