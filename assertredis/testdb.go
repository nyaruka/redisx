package assertredis

import (
	"context"
	"fmt"
	"os"

	"github.com/nyaruka/redisx"
	"github.com/valkey-io/valkey-go"
)

const (
	// maybe don't run these tests where you store your production database
	testDBIndex = 0
)

// TestDB returns a redis pool to our test database
func TestDB() redisx.Pool {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{getHostAddress()},
		SelectDB:    testDBIndex,
	})
	if err != nil {
		panic(fmt.Sprintf("error creating valkey client: %s", err.Error()))
	}

	return redisx.NewValkeyPool(client)
}

// FlushDB flushes the test database
func FlushDB() {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{getHostAddress()},
		SelectDB:    testDBIndex,
	})
	if err != nil {
		panic(fmt.Sprintf("error connecting to redis db: %s", err.Error()))
	}
	defer client.Close()

	cmd := client.B().Flushdb().Build()
	result := client.Do(context.Background(), cmd)
	if result.Error() != nil {
		panic(fmt.Sprintf("error flushing redis db: %s", result.Error()))
	}
}

func getHostAddress() string {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	return host + ":6379"
}
