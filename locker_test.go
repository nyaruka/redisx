package vkutil_test

import (
	"context"
	"testing"
	"time"

	vkutil "github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	ctx := context.Background()
	rp := assertvk.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertvk.FlushDB()

	locker := vkutil.NewLocker("test", time.Second*5)

	isLocked, err := locker.IsLocked(ctx, rp)
	assert.NoError(t, err)
	assert.False(t, isLocked)

	// grab lock
	lock1, err := locker.Grab(ctx, rp, time.Second)
	assert.NoError(t, err)
	assert.NotZero(t, lock1)

	isLocked, err = locker.IsLocked(ctx, rp)
	assert.NoError(t, err)
	assert.True(t, isLocked)

	assertvk.Exists(t, rc, "test")

	// try to acquire the same lock, should fail
	lock2, err := locker.Grab(ctx, rp, time.Second)
	assert.NoError(t, err)
	assert.Zero(t, lock2)

	// should succeed if we wait longer
	lock3, err := locker.Grab(ctx, rp, time.Second*6)
	assert.NoError(t, err)
	assert.NotZero(t, lock3)
	assert.NotEqual(t, lock1, lock3)

	// extend the lock
	err = locker.Extend(ctx, rp, lock3, time.Second*10)
	assert.NoError(t, err)

	// trying to grab it should fail with a 5 second timeout
	lock4, err := locker.Grab(ctx, rp, time.Second*5)
	assert.NoError(t, err)
	assert.Zero(t, lock4)

	// try to release the lock with wrong value
	err = locker.Release(ctx, rp, "2352")
	assert.NoError(t, err)

	// no error but also dooesn't release the lock
	assertvk.Exists(t, rc, "test")

	// release the lock
	err = locker.Release(ctx, rp, lock3)
	assert.NoError(t, err)

	assertvk.NotExists(t, rc, "test")

	// new grab should work
	lock5, err := locker.Grab(ctx, rp, time.Second*5)
	assert.NoError(t, err)
	assert.NotZero(t, lock5)

	assertvk.Exists(t, rc, "test")
}
