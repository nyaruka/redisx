package redisx

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// WithMaxActive configures maximum number of concurrent connections to allow
// Note: valkey-go handles connection pooling internally, so this is a no-op for compatibility
func WithMaxActive(v int) func(Pool) {
	return func(p Pool) {
		// valkey-go handles connection pooling internally
	}
}

// WithMaxIdle configures the maximum number of idle connections to keep
// Note: valkey-go handles connection pooling internally, so this is a no-op for compatibility
func WithMaxIdle(v int) func(Pool) {
	return func(p Pool) {
		// valkey-go handles connection pooling internally
	}
}

// WithIdleTimeout configures how long to wait before reaping a connection
// Note: valkey-go handles connection pooling internally, so this is a no-op for compatibility
func WithIdleTimeout(v time.Duration) func(Pool) {
	return func(p Pool) {
		// valkey-go handles connection pooling internally
	}
}

// NewPool creates a new pool with the given options
func NewPool(redisURL string, options ...func(Pool)) (Pool, error) {
	parsedURL, err := url.Parse(redisURL)
	if err != nil {
		return nil, err
	}

	// Parse connection options
	clientOptions := valkey.ClientOption{
		InitAddress: []string{parsedURL.Host},
	}

	// Handle auth if present
	if parsedURL.User != nil {
		pass, authRequired := parsedURL.User.Password()
		if authRequired {
			clientOptions.Password = pass
		}
		if user := parsedURL.User.Username(); user != "" {
			clientOptions.Username = user
		}
	}

	// Handle database selection
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		dbStr := strings.TrimLeft(parsedURL.Path, "/")
		if dbStr != "" {
			db, err := strconv.Atoi(dbStr)
			if err != nil {
				return nil, err
			}
			clientOptions.SelectDB = db
		}
	}

	// Create valkey client
	client, err := valkey.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}

	// Create pool wrapper
	pool := NewValkeyPool(client)

	// Apply options (they're no-ops for valkey but kept for compatibility)
	for _, o := range options {
		o(pool)
	}

	// Test the connection
	conn := pool.Get()
	defer conn.Close()
	if _, err = conn.Do("PING"); err != nil {
		client.Close()
		return nil, err
	}

	return pool, nil
}
