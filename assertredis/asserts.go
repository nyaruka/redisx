package assertredis

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
)

// Keys asserts that only the given keys exist
func Keys(t *testing.T, client valkey.Client, pattern string, expected []string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Keys().Pattern(pattern).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	arr, err := result.ToArray()
	assert.NoError(t, err)
	
	actual := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		assert.NoError(t, err)
		actual[i] = str
	}

	return assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// Exists asserts that the given key exists
func Exists(t *testing.T, client valkey.Client, key string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Exists().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	exists := count > 0

	if !exists {
		assert.Fail(t, "Key should exist", msgAndArgs...)
	}

	return exists
}

// NotExists asserts that the given key does not exist
func NotExists(t *testing.T, client valkey.Client, key string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Exists().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	exists := count > 0

	if exists {
		assert.Fail(t, "Key should not exist", msgAndArgs...)
	}

	return !exists
}

// Get asserts that the given key contains the given string value
func Get(t *testing.T, client valkey.Client, key string, expected string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Get().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	actual, err := result.ToString()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// SCard asserts the result of calling SCARD on the given key
func SCard(t *testing.T, client valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Scard().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	actual := int(count)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// SIsMember asserts the result that calling SISMEMBER on the given key is true
func SIsMember(t *testing.T, client valkey.Client, key, member string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Sismember().Key(key).Member(member).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	exists := count > 0

	if !exists {
		assert.Fail(t, "Key should be member", msgAndArgs...)
	}

	return exists
}

// SIsNotMember asserts the result of calling SISMEMBER on the given key is false
func SIsNotMember(t *testing.T, client valkey.Client, key, member string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Sismember().Key(key).Member(member).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	exists := count > 0

	if exists {
		assert.Fail(t, "Key should not be member", msgAndArgs...)
	}

	return !exists
}

// SMembers asserts the result of calling SMEMBERS on the given key
func SMembers(t *testing.T, client valkey.Client, key string, expected []string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Smembers().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	arr, err := result.ToArray()
	assert.NoError(t, err)
	
	actual := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		assert.NoError(t, err)
		actual[i] = str
	}

	return assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// HGetAll asserts the result of calling HGET on the given key and field
func HGet(t *testing.T, client valkey.Client, key, field string, expected string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Hget().Key(key).Field(field).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	actual, err := result.ToString()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// HGetAll asserts the result of calling HGETALL on the given key
func HGetAll(t *testing.T, client valkey.Client, key string, expected map[string]string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Hgetall().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	actual, err := result.ToMap()
	assert.NoError(t, err)
	
	actualStrings := make(map[string]string, len(actual))
	for k, v := range actual {
		str, err := v.ToString()
		assert.NoError(t, err)
		actualStrings[k] = str
	}

	return assert.Equal(t, expected, actualStrings, msgAndArgs...)
}

// HLen asserts the result of calling HLEN on the given key
func HLen(t *testing.T, client valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Hlen().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	actual := int(count)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LLen asserts the result of calling LLEN on the given key
func LLen(t *testing.T, client valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Llen().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	actual := int(count)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LRange asserts the result of calling LRANGE on the given key
func LRange(t *testing.T, client valkey.Client, key string, start, stop int, expected []string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Lrange().Key(key).Start(int64(start)).Stop(int64(stop)).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	arr, err := result.ToArray()
	assert.NoError(t, err)
	
	actual := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		assert.NoError(t, err)
		actual[i] = str
	}

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LGetAll asserts the result of calling LRANGE <?> 0 -1 on the given key
func LGetAll(t *testing.T, client valkey.Client, key string, expected []string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Lrange().Key(key).Start(0).Stop(-1).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	arr, err := result.ToArray()
	assert.NoError(t, err)
	
	actual := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		assert.NoError(t, err)
		actual[i] = str
	}

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZCard asserts the result of calling ZCARD on the given key
func ZCard(t *testing.T, client valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Zcard().Key(key).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	count, err := result.ToInt64()
	assert.NoError(t, err)
	actual := int(count)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZRange asserts the result of calling ZRANGE on the given key
func ZRange(t *testing.T, client valkey.Client, key string, start, stop int, expected []string, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Zrange().Key(key).Min(strconv.Itoa(start)).Max(strconv.Itoa(stop)).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	arr, err := result.ToArray()
	assert.NoError(t, err)
	
	actual := make([]string, len(arr))
	for i, item := range arr {
		str, err := item.ToString()
		assert.NoError(t, err)
		actual[i] = str
	}

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZGetAll asserts the result of calling ZRANGE <?> 0 -1 WITHSCORES on the given key
func ZGetAll(t *testing.T, client valkey.Client, key string, expected map[string]float64, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Zrange().Key(key).Min("0").Max("-1").Withscores().Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	arr, err := result.ToArray()
	assert.NoError(t, err)
	
	actual := make(map[string]float64)
	for i := 0; i < len(arr)/2; i++ {
		member, err := arr[2*i].ToString()
		assert.NoError(t, err)
		
		scoreStr, err := arr[2*i+1].ToString()
		assert.NoError(t, err)
		
		score, err := strconv.ParseFloat(scoreStr, 64)
		require.NoError(t, err)
		
		actual[member] = score
	}

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZRange asserts the result of calling ZSCORE on the given key
func ZScore(t *testing.T, client valkey.Client, key, member string, expected float64, msgAndArgs ...any) bool {
	ctx := context.Background()
	cmd := client.B().Zscore().Key(key).Member(member).Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
	
	actual, err := result.ToFloat64()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}
