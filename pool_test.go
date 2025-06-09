package redisx_test

import (
	"testing"
	"time"

	"github.com/nyaruka/redisx"
	"github.com/stretchr/testify/assert"
)

func TestNewPool(t *testing.T) {
	// test basic pool creation - valkey handles pooling internally so we just test connectivity
	rp, err := redisx.NewPool("redis://redis6:6379/15")
	if err != nil {
		t.Skipf("Redis not available: %v", err)
		return
	}
	
	// test that pool options are accepted (even if they're no-ops in valkey)
	rp, err = redisx.NewPool("redis://redis6:6379/15", redisx.WithMaxActive(10), redisx.WithMaxIdle(3), redisx.WithIdleTimeout(time.Minute))
	if err != nil {
		t.Skipf("Redis not available: %v", err)
		return
	}
	
	// test that we can get a connection and execute a command
	conn := rp.Get()
	defer conn.Close()
	
	_, err = conn.Do("PING")
	assert.NoError(t, err)
}
