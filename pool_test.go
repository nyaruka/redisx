package redisx_test

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/redisx"
	"github.com/stretchr/testify/assert"
)

func TestNewPool(t *testing.T) {
	// test basic pool creation - valkey handles pooling internally so we just test connectivity
	client, err := redisx.NewPool("redis://redis6:6379/15")
	if err != nil {
		t.Skipf("Redis not available: %v", err)
		return
	}
	defer client.Close()
	
	// test that pool options are accepted (even if they're no-ops in valkey)
	client2, err := redisx.NewPool("redis://redis6:6379/15", redisx.WithMaxActive(10), redisx.WithMaxIdle(3), redisx.WithIdleTimeout(time.Minute))
	if err != nil {
		t.Skipf("Redis not available: %v", err)
		return
	}
	defer client2.Close()
	
	// test that we can execute a command
	ctx := context.Background()
	cmd := client.B().Ping().Build()
	result := client.Do(ctx, cmd)
	assert.NoError(t, result.Error())
}
