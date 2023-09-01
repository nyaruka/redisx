package redisx_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/redisx"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalHash(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	setNow := func(d time.Time) { dates.SetNowSource(dates.NewFixedNowSource(d)) }

	setNow(time.Date(2021, 11, 18, 12, 7, 3, 234567, time.UTC))

	assertGet := func(h *redisx.IntervalHash, k, expected string) {
		actual, err := h.Get(rc, k)
		assert.NoError(t, err, "unexpected error getting key %s", k)
		assert.Equal(t, expected, actual, "expected hash key %s to contain %s", k, expected)
	}
	assertMGet := func(h *redisx.IntervalHash, ks []string, expected []string) {
		actual, err := h.MGet(rc, ks...)
		assert.NoError(t, err, "unexpected error getting keys %s", strings.Join(ks, ","))
		assert.Equal(t, expected, actual, "expected hash keys %s to contain %s", strings.Join(ks, ","), strings.Join(expected, ","))
	}

	// create a 24-hour x 2 based hash
	cache1 := redisx.NewIntervalHash("foos", time.Hour*24, 2)
	assert.NoError(t, cache1.Set(rc, "A", "1"))
	assert.NoError(t, cache1.Set(rc, "B", "2"))
	assert.NoError(t, cache1.Set(rc, "C", "3"))

	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "1")
	assertGet(cache1, "B", "2")
	assertGet(cache1, "C", "3")
	assertGet(cache1, "D", "")
	assertMGet(cache1, []string{"A", "C", "D"}, []string{"1", "3", ""})
	assertMGet(cache1, []string{"D", "A"}, []string{"", "1"})

	_, err := cache1.MGet(rc) // zero fields is an error
	assert.EqualError(t, err, "wrong number of arguments for command")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 7, 3, 234567, time.UTC))

	cache1.Set(rc, "A", "5")
	cache1.Set(rc, "B", "6")

	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "5")
	assertGet(cache1, "B", "6")
	assertGet(cache1, "C", "3")
	assertGet(cache1, "D", "")
	assertMGet(cache1, []string{"A", "C", "D"}, []string{"5", "3", ""})
	assertMGet(cache1, []string{"B"}, []string{"6"})

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	cache1.Set(rc, "A", "7")
	cache1.Set(rc, "Z", "9")

	assertredis.HGetAll(t, rp, "foos:2021-11-20", map[string]string{"A": "7", "Z": "9"})
	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "7")
	assertGet(cache1, "Z", "9")
	assertGet(cache1, "B", "6")
	assertGet(cache1, "C", "") // too old
	assertGet(cache1, "D", "")
	assertMGet(cache1, []string{"B", "A", "D"}, []string{"6", "7", ""})

	err = cache1.Remove(rc, "A") // from today and yesterday
	require.NoError(t, err)
	err = cache1.Remove(rc, "B") // from yesterday
	require.NoError(t, err)

	assertredis.HGetAll(t, rp, "foos:2021-11-20", map[string]string{"Z": "9"})
	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{})
	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "")
	assertGet(cache1, "Z", "9")
	assertGet(cache1, "B", "")
	assertGet(cache1, "C", "")
	assertGet(cache1, "D", "")

	err = cache1.ClearAll(rc)
	require.NoError(t, err)

	assertredis.HGetAll(t, rp, "foos:2021-11-20", map[string]string{})
	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{})

	assertGet(cache1, "A", "")
	assertGet(cache1, "Z", "")
	assertGet(cache1, "B", "")
	assertGet(cache1, "C", "")
	assertGet(cache1, "D", "")

	// create a 5 minute x 3 based hash
	cache2 := redisx.NewIntervalHash("foos", time.Minute*5, 3)
	cache2.Set(rc, "A", "1")
	cache2.Set(rc, "B", "2")

	assertredis.HGetAll(t, rp, "foos:2021-11-20T12:05", map[string]string{"A": "1", "B": "2"})
	assertredis.HGetAll(t, rp, "foos:2021-11-20T12:00", map[string]string{})

	assertGet(cache2, "A", "1")
	assertGet(cache2, "B", "2")
	assertGet(cache2, "C", "")

	// create a 5 second x 2 based set
	cache3 := redisx.NewIntervalHash("foos", time.Second*5, 2)
	cache3.Set(rc, "A", "1")
	cache3.Set(rc, "B", "2")

	assertredis.HGetAll(t, rp, "foos:2021-11-20T12:07:00", map[string]string{"A": "1", "B": "2"})
	assertredis.HGetAll(t, rp, "foos:2021-11-20T12:06:55", map[string]string{})

	assertGet(cache3, "A", "1")
	assertGet(cache3, "B", "2")
	assertGet(cache3, "C", "")
}
