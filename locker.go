package redisx

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Locker is a lock implementation where grabbing returns a lock value and that value must be
// used to release or extend the lock.
type Locker struct {
	key        string
	expiration time.Duration
}

// NewLocker creates a new locker using the given key and expiration
func NewLocker(key string, expiration time.Duration) *Locker {
	return &Locker{key: key, expiration: expiration}
}

// Grab tries to grab this lock in an atomic operation. It returns the lock value if successful.
// It will retry every second until the retry period has ended, returning empty string if not
// acquired in that time.
func (l *Locker) Grab(client valkey.Client, retry time.Duration) (string, error) {
	value := RandomBase64(10)                  // generate our lock value
	expires := int(l.expiration / time.Second) // convert our expiration to seconds

	ctx := context.Background()
	start := time.Now()
	for {
		cmd := client.B().Set().Key(l.key).Value(value).Nx().ExSeconds(int64(expires)).Build()
		result := client.Do(ctx, cmd)

		if result.Error() != nil {
			return "", fmt.Errorf("error trying to get lock: %w", result.Error())
		}
		
		// Check if SET was successful (returns "OK" when successful with NX)
		str, err := result.ToString()
		if err == nil && str == "OK" {
			break
		}

		if time.Since(start) > retry {
			return "", nil
		}

		time.Sleep(time.Second)
	}

	return value, nil
}

//go:embed lua/locker_release.lua
var lockerRelease string
var lockerReleaseScript = valkey.NewLuaScript(lockerRelease)

// Release releases this lock if the given lock value is correct (i.e we own this lock). It is not an
// error to release a lock that is no longer present.
func (l *Locker) Release(client valkey.Client, value string) error {
	ctx := context.Background()
	
	// we use lua here because we only want to release the lock if we own it
	result := lockerReleaseScript.Exec(ctx, client, []string{l.key}, []string{value})
	return result.Error()
}

//go:embed lua/locker_extend.lua
var lockerExtend string
var lockerExtendScript = valkey.NewLuaScript(lockerExtend)

// Extend extends our lock expiration by the passed in number of seconds provided the lock value is correct
func (l *Locker) Extend(client valkey.Client, value string, expiration time.Duration) error {
	ctx := context.Background()
	seconds := int(expiration / time.Second) // convert our expiration to seconds

	// we use lua here because we only want to set the expiration time if we own it
	result := lockerExtendScript.Exec(ctx, client, []string{l.key}, []string{value, fmt.Sprintf("%d", seconds)})
	return result.Error()
}

// IsLocked returns whether this lock is currently held by any process.
func (l *Locker) IsLocked(client valkey.Client) (bool, error) {
	ctx := context.Background()
	cmd := client.B().Exists().Key(l.key).Build()
	result := client.Do(ctx, cmd)
	
	if result.Error() != nil {
		return false, result.Error()
	}

	count, err := result.ToInt64()
	if err != nil {
		return false, err
	}

	return count > 0, nil
}
