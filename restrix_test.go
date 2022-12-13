package restrix

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/stretchr/testify/assert"

	"github.com/gomodule/redigo/redis"

	"github.com/stretchr/testify/require"
)

func TestBreaker_DoCtx(t *testing.T) {
	mrServer, err := miniredis.Run()
	require.NoError(t, err)
	defer mrServer.Close()

	mrAddr := mrServer.Addr()
	circuitSetting := CircuitSetting{
		RequestCountThreshold: 2,
		SleepWindow:           10 * time.Second,
		ErrorPercentThreshold: 50,
		Interval:              10 * time.Second,
	}

	redisPool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", mrAddr) },
	}

	t.Run("circuit closed", func(t *testing.T) {
		myName := "success_test_case"
		breaker := NewBreaker(redisPool, circuitSetting)

		err = breaker.DoCtx(context.TODO(), myName, func(ctx context.Context) error {
			return nil
		})
		require.NoError(t, err)

		ns := newNamespacer(myName)
		reply, err := mrServer.Get(ns.requestCount())
		require.NoError(t, err)
		assert.Equal(t, "1", reply)

		reply, err = mrServer.Get(ns.currentState())
		require.NoError(t, err)
		assert.Equal(t, CircuitStateClosed, reply)
	})

	t.Run("noflip - below request count threshold", func(t *testing.T) {
		myName := "error_below_request_count_threshold"

		breaker := NewBreaker(redisPool, circuitSetting)

		err = breaker.DoCtx(context.TODO(), myName, func(ctx context.Context) error {
			return errors.New("error nih")
		})
		require.NoError(t, err)

		ns := newNamespacer(myName)
		reply, err := mrServer.Get(ns.requestCount())
		require.NoError(t, err)
		assert.Equal(t, "1", reply)

		reply, err = mrServer.Get(ns.errorCount())
		require.NoError(t, err)
		assert.Equal(t, "1", reply)

		reply, err = mrServer.Get(ns.currentState())
		require.NoError(t, err)
		assert.Equal(t, CircuitStateClosed, reply)
	})

	t.Run("noflip - above request count threshold but below error percentage", func(t *testing.T) {
		myName := "error_above_request_count_threshold_but_below_error_percentage"

		breaker := NewBreaker(redisPool, CircuitSetting{
			RequestCountThreshold: 2,
			SleepWindow:           10 * time.Second,
			ErrorPercentThreshold: 50,
			Interval:              10 * time.Second,
		})

		ns := newNamespacer(myName)
		err = mrServer.Set(ns.requestCount(), "1000")
		require.NoError(t, err)

		err = breaker.DoCtx(context.TODO(), myName, func(ctx context.Context) error {
			return errors.New("error nih")
		})
		require.NoError(t, err)

		reply, err := mrServer.Get(ns.requestCount())
		require.NoError(t, err)
		assert.Equal(t, "1001", reply)

		reply, err = mrServer.Get(ns.errorCount())
		require.NoError(t, err)
		assert.Equal(t, "1", reply)

		reply, err = mrServer.Get(ns.currentState())
		require.NoError(t, err)
		assert.Equal(t, CircuitStateClosed, reply)
	})

	t.Run("flip open", func(t *testing.T) {
		myName := "flip_open"
		breaker := NewBreaker(redisPool, circuitSetting)

		ns := newNamespacer(myName)
		err = mrServer.Set(ns.requestCount(), "1")
		require.NoError(t, err)

		err = mrServer.Set(ns.errorCount(), "1")
		require.NoError(t, err)

		err = breaker.DoCtx(context.TODO(), myName, func(ctx context.Context) error {
			return errors.New("error nih")
		})
		require.NoError(t, err)

		reply, err := mrServer.Get(ns.requestCount())
		require.NoError(t, err)
		assert.Equal(t, "2", reply)

		reply, err = mrServer.Get(ns.currentState())
		require.NoError(t, err)
		assert.Equal(t, CircuitStateOpened, reply)

		ttl := mrServer.TTL(ns.openStateTTL())
		assert.GreaterOrEqual(t, breaker.sleepWindow, ttl)
		assert.Greater(t, ttl, 0*time.Second)
	})

	t.Run("half open - still open", func(t *testing.T) {
		myName := "half_open_noflip"
		breaker := NewBreaker(redisPool, circuitSetting)

		ns := newNamespacer(myName)
		err = mrServer.Set(ns.currentState(), CircuitStateOpened)
		require.NoError(t, err)

		err = breaker.DoCtx(context.TODO(), myName, func(ctx context.Context) error {
			return errors.New("error nih")
		})
		require.NoError(t, err)

		reply, err := mrServer.Get(ns.currentState())
		require.NoError(t, err)
		assert.Equal(t, CircuitStateOpened, reply)

		ttl := mrServer.TTL(ns.openStateTTL())
		assert.GreaterOrEqual(t, breaker.sleepWindow, ttl)
		assert.Greater(t, ttl, 0*time.Second)
	})

	t.Run("half open - flip close", func(t *testing.T) {
		myName := "half_open_flip"
		breaker := NewBreaker(redisPool, circuitSetting)

		ns := newNamespacer(myName)
		err = mrServer.Set(ns.currentState(), CircuitStateOpened)
		require.NoError(t, err)

		err = breaker.DoCtx(context.TODO(), myName, func(ctx context.Context) error {
			return nil
		})
		require.NoError(t, err)

		reply, err := mrServer.Get(ns.currentState())
		require.NoError(t, err)
		assert.Equal(t, CircuitStateClosed, reply)
	})
}
