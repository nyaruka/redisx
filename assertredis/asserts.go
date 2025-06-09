package assertredis

import (
	"strconv"
	"testing"

	"github.com/nyaruka/redisx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keys asserts that only the given keys exist
func Keys(t *testing.T, rc redisx.Conn, pattern string, expected []string, msgAndArgs ...any) bool {
	actual, err := redisx.Strings(rc.Do("KEYS", pattern))
	assert.NoError(t, err)

	return assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// Exists asserts that the given key exists
func Exists(t *testing.T, rc redisx.Conn, key string, msgAndArgs ...any) bool {
	exists, err := redisx.Bool(rc.Do("EXISTS", key))
	assert.NoError(t, err)

	if !exists {
		assert.Fail(t, "Key should exist", msgAndArgs...)
	}

	return exists
}

// NotExists asserts that the given key does not exist
func NotExists(t *testing.T, rc redisx.Conn, key string, msgAndArgs ...any) bool {
	exists, err := redisx.Bool(rc.Do("EXISTS", key))
	assert.NoError(t, err)

	if exists {
		assert.Fail(t, "Key should not exist", msgAndArgs...)
	}

	return !exists
}

// Get asserts that the given key contains the given string value
func Get(t *testing.T, rc redisx.Conn, key string, expected string, msgAndArgs ...any) bool {
	actual, err := redisx.String(rc.Do("GET", key))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// SCard asserts the result of calling SCARD on the given key
func SCard(t *testing.T, rc redisx.Conn, key string, expected int, msgAndArgs ...any) bool {
	actual, err := redisx.Int(rc.Do("SCARD", key))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// SIsMember asserts the result that calling SISMEMBER on the given key is true
func SIsMember(t *testing.T, rc redisx.Conn, key, member string, msgAndArgs ...any) bool {
	exists, err := redisx.Bool(rc.Do("SISMEMBER", key, member))
	assert.NoError(t, err)

	if !exists {
		assert.Fail(t, "Key should be member", msgAndArgs...)
	}

	return exists
}

// SIsNotMember asserts the result of calling SISMEMBER on the given key is false
func SIsNotMember(t *testing.T, rc redisx.Conn, key, member string, msgAndArgs ...any) bool {
	exists, err := redisx.Bool(rc.Do("SISMEMBER", key, member))
	assert.NoError(t, err)

	if exists {
		assert.Fail(t, "Key should not be member", msgAndArgs...)
	}

	return !exists
}

// SMembers asserts the result of calling SMEMBERS on the given key
func SMembers(t *testing.T, rc redisx.Conn, key string, expected []string, msgAndArgs ...any) bool {
	actual, err := redisx.Strings(rc.Do("SMEMBERS", key))
	assert.NoError(t, err)

	return assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// HGetAll asserts the result of calling HGET on the given key and field
func HGet(t *testing.T, rc redisx.Conn, key, field string, expected string, msgAndArgs ...any) bool {
	actual, err := redisx.String(rc.Do("HGET", key, field))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// HGetAll asserts the result of calling HGETALL on the given key
func HGetAll(t *testing.T, rc redisx.Conn, key string, expected map[string]string, msgAndArgs ...any) bool {
	actual, err := redisx.StringMap(rc.Do("HGETALL", key))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// HLen asserts the result of calling HLEN on the given key
func HLen(t *testing.T, rc redisx.Conn, key string, expected int, msgAndArgs ...any) bool {
	actual, err := redisx.Int(rc.Do("HLEN", key))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LLen asserts the result of calling LLEN on the given key
func LLen(t *testing.T, rc redisx.Conn, key string, expected int, msgAndArgs ...any) bool {
	actual, err := redisx.Int(rc.Do("LLEN", key))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LRange asserts the result of calling LRANGE on the given key
func LRange(t *testing.T, rc redisx.Conn, key string, start, stop int, expected []string, msgAndArgs ...any) bool {
	actual, err := redisx.Strings(rc.Do("LRANGE", key, start, stop))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LGetAll asserts the result of calling LRANGE <?> 0 -1 on the given key
func LGetAll(t *testing.T, rc redisx.Conn, key string, expected []string, msgAndArgs ...any) bool {
	actual, err := redisx.Strings(rc.Do("LRANGE", key, 0, -1))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZCard asserts the result of calling ZCARD on the given key
func ZCard(t *testing.T, rc redisx.Conn, key string, expected int, msgAndArgs ...any) bool {
	actual, err := redisx.Int(rc.Do("ZCARD", key))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZRange asserts the result of calling ZRANGE on the given key
func ZRange(t *testing.T, rc redisx.Conn, key string, start, stop int, expected []string, msgAndArgs ...any) bool {
	actual, err := redisx.Strings(rc.Do("ZRANGE", key, start, stop))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZGetAll asserts the result of calling ZRANGE <?> 0 -1 WITHSCORES on the given key
func ZGetAll(t *testing.T, rc redisx.Conn, key string, expected map[string]float64, msgAndArgs ...any) bool {
	actualStrings, err := redisx.StringMap(rc.Do("ZRANGE", key, 0, -1, "WITHSCORES"))
	assert.NoError(t, err)

	actual := make(map[string]float64, len(actualStrings))
	for k, v := range actualStrings {
		actual[k], err = strconv.ParseFloat(v, 64)
		require.NoError(t, err)
	}

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZRange asserts the result of calling ZSCORE on the given key
func ZScore(t *testing.T, rc redisx.Conn, key, member string, expected float64, msgAndArgs ...any) bool {
	actual, err := redisx.Float64(rc.Do("ZSCORE", key, member))
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}
