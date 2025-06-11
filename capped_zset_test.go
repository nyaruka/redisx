package redisx_test

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/redisx"
	"github.com/nyaruka/redisx/assertvk"
	"github.com/stretchr/testify/assert"
)

func TestCappedZSet(t *testing.T) {
	ctx := context.Background()
	rp := assertvk.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertvk.FlushDB()

	assertMembers := func(s *redisx.CappedZSet, expectedMembers []string, expectedScores []float64) {
		actualMembers, actualScores, err := s.Members(ctx, rc)
		assert.NoError(t, err)
		assert.Equal(t, expectedMembers, actualMembers)
		assert.Equal(t, expectedScores, actualScores)
	}

	zset := redisx.NewCappedZSet("foo", 3, time.Minute*5)
	assert.NoError(t, zset.Add(ctx, rc, "A", 1))
	assert.NoError(t, zset.Add(ctx, rc, "C", 3))
	assert.NoError(t, zset.Add(ctx, rc, "B", 2))

	assertvk.ZGetAll(t, rc, "foo", map[string]float64{"A": 1, "B": 2, "C": 3})

	card, err := zset.Card(ctx, rc)
	assert.NoError(t, err)
	assert.Equal(t, 3, card)

	assertMembers(zset, []string{"A", "B", "C"}, []float64{1, 2, 3})

	// adding a new member with a higher score, pushes out the lowest scoring element
	zset.Add(ctx, rc, "D", 4)

	assertMembers(zset, []string{"B", "C", "D"}, []float64{2, 3, 4})

	// adding a new member with a non-unique score still maintains the cap
	zset.Add(ctx, rc, "E", 4)

	assertMembers(zset, []string{"C", "D", "E"}, []float64{3, 4, 4})

	// adding a new member with a score that's too low is a noop
	zset.Add(ctx, rc, "F", 2)

	assertMembers(zset, []string{"C", "D", "E"}, []float64{3, 4, 4})

	// order is always based on score rather than lex
	zset.Add(ctx, rc, "G", 3.5)

	assertMembers(zset, []string{"G", "D", "E"}, []float64{3.5, 4, 4})

	// re-adding a member updates the score
	zset.Add(ctx, rc, "D", 4.5)

	assertMembers(zset, []string{"G", "E", "D"}, []float64{3.5, 4, 4.5})
}
