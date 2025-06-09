package redisx_test

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/redisx"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalSet(t *testing.T) {
	ctx := context.Background()
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowFunc(time.Now)
	setNow := func(d time.Time) { dates.SetNowFunc(dates.NewFixedNow(d)) }

	setNow(time.Date(2021, 11, 18, 12, 0, 3, 234567, time.UTC))

	// create a 24-hour x 2 based set
	set1 := redisx.NewIntervalSet("foos", time.Hour*24, 2)
	assert.NoError(t, set1.Add(ctx, rc, "A"))
	assert.NoError(t, set1.Add(ctx, rc, "B"))
	assert.NoError(t, set1.Add(ctx, rc, "C"))

	assertredis.SMembers(t, rc, "foos:2021-11-18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rc, "foos:2021-11-17", []string{})

	assertIsMember := func(s *redisx.IntervalSet, v string) {
		contains, err := s.IsMember(ctx, rc, v)
		assert.NoError(t, err)
		assert.True(t, contains, "expected marker to contain %s", v)
	}
	assertNotIsMember := func(s *redisx.IntervalSet, v string) {
		contains, err := s.IsMember(ctx, rc, v)
		assert.NoError(t, err)
		assert.False(t, contains, "expected marker to not contain %s", v)
	}

	assertIsMember(set1, "A")
	assertIsMember(set1, "B")
	assertIsMember(set1, "C")
	assertNotIsMember(set1, "D")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 0, 3, 234567, time.UTC))

	set1.Add(ctx, rc, "D")
	set1.Add(ctx, rc, "E")

	assertredis.SMembers(t, rc, "foos:2021-11-19", []string{"D", "E"})
	assertredis.SMembers(t, rc, "foos:2021-11-18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rc, "foos:2021-11-17", []string{})

	assertIsMember(set1, "A")
	assertIsMember(set1, "B")
	assertIsMember(set1, "C")
	assertIsMember(set1, "D")
	assertIsMember(set1, "E")
	assertNotIsMember(set1, "F")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	set1.Add(ctx, rc, "F")
	set1.Add(ctx, rc, "G")

	assertredis.SMembers(t, rc, "foos:2021-11-20", []string{"F", "G"})
	assertredis.SMembers(t, rc, "foos:2021-11-19", []string{"D", "E"})
	assertredis.SMembers(t, rc, "foos:2021-11-18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rc, "foos:2021-11-17", []string{})

	assertNotIsMember(set1, "A") // too old
	assertNotIsMember(set1, "B") // too old
	assertNotIsMember(set1, "C") // too old
	assertIsMember(set1, "D")
	assertIsMember(set1, "E")
	assertIsMember(set1, "F")
	assertIsMember(set1, "G")

	err := set1.Rem(ctx, rc, "F") // from today
	require.NoError(t, err)
	err = set1.Rem(ctx, rc, "E") // from yesterday
	require.NoError(t, err)

	assertredis.SMembers(t, rc, "foos:2021-11-20", []string{"G"})
	assertredis.SMembers(t, rc, "foos:2021-11-19", []string{"D"})

	assertIsMember(set1, "D")
	assertNotIsMember(set1, "E")
	assertNotIsMember(set1, "F")
	assertIsMember(set1, "G")

	err = set1.Clear(ctx, rc)
	require.NoError(t, err)

	assertredis.SMembers(t, rc, "foos:2021-11-20", []string{})
	assertredis.SMembers(t, rc, "foos:2021-11-19", []string{})

	assertNotIsMember(set1, "D")
	assertNotIsMember(set1, "E")
	assertNotIsMember(set1, "F")
	assertNotIsMember(set1, "G")

	// create a 5 minute x 3 based set
	set2 := redisx.NewIntervalSet("foos", time.Minute*5, 3)
	set2.Add(ctx, rc, "A")
	set2.Add(ctx, rc, "B")

	assertredis.SMembers(t, rc, "foos:2021-11-20T12:05", []string{"A", "B"})
	assertredis.SMembers(t, rc, "foos:2021-11-20T12:00", []string{})

	assertIsMember(set2, "A")
	assertIsMember(set2, "B")
	assertNotIsMember(set2, "C")

	// create a 5 second x 2 based set
	set3 := redisx.NewIntervalSet("foos", time.Second*5, 2)
	set3.Add(ctx, rc, "A")
	set3.Add(ctx, rc, "B")

	assertredis.SMembers(t, rc, "foos:2021-11-20T12:07:00", []string{"A", "B"})
	assertredis.SMembers(t, rc, "foos:2021-11-20T12:06:55", []string{})

	assertIsMember(set3, "A")
	assertIsMember(set3, "B")
	assertNotIsMember(set3, "C")
}
