package redisx_test

import (
	"testing"
	"time"

	"github.com/nyaruka/redisx"
	"github.com/stretchr/testify/assert"
)

func TestNewPool(t *testing.T) {
	// check defaults
	rp, err := redisx.NewPool("redis://redis6:6379/15")
	assert.NoError(t, err)
	assert.Equal(t, 32, rp.MaxActive)
	assert.Equal(t, 4, rp.MaxIdle)
	assert.Equal(t, 180*time.Second, rp.IdleTimeout)

	rp, err = redisx.NewPool("redis://redis6:6379/15", redisx.WithMaxActive(10), redisx.WithMaxIdle(3), redisx.WithIdleTimeout(time.Minute))
	assert.NoError(t, err)
	assert.Equal(t, 10, rp.MaxActive)
	assert.Equal(t, 3, rp.MaxIdle)
	assert.Equal(t, time.Minute, rp.IdleTimeout)
}
