package redisx

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// WithMaxActive configures maximum number of concurrent connections to allow
// Note: valkey-go handles connection pooling internally, so this is a no-op for compatibility
func WithMaxActive(v int) func(valkey.Client) {
	return func(c valkey.Client) {
		// valkey-go handles connection pooling internally
	}
}

// WithMaxIdle configures the maximum number of idle connections to keep
// Note: valkey-go handles connection pooling internally, so this is a no-op for compatibility
func WithMaxIdle(v int) func(valkey.Client) {
	return func(c valkey.Client) {
		// valkey-go handles connection pooling internally
	}
}

// WithIdleTimeout configures how long to wait before reaping a connection
// Note: valkey-go handles connection pooling internally, so this is a no-op for compatibility
func WithIdleTimeout(v time.Duration) func(valkey.Client) {
	return func(c valkey.Client) {
		// valkey-go handles connection pooling internally
	}
}

// NewPool creates a new pool with the given options
func NewPool(ctx context.Context, redisURL string, options ...func(valkey.Client)) (valkey.Client, error) {
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

	// Apply options (they're no-ops for valkey but kept for compatibility)
	for _, o := range options {
		o(client)
	}

	// Test the connection
	result := client.Do(ctx, client.B().Ping().Build())
	if result.Error() != nil {
		client.Close()
		return nil, result.Error()
	}

	return client, nil
}
