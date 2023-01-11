package restrix

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

type (
	// CircuitSettings is used to tune circuit settings at runtime
	CircuitSettings struct {
		// RequestCountThreshold is the minimum number of requests needed cbefore a circuit can be tripped due to health
		RequestCountThreshold int `json:"request_volume_threshold"`

		// SleepWindow is how long to wait after a circuit opens before testing for recovery
		SleepWindow time.Duration `json:"sleep_window"`

		// ErrorPercentThreshold causes circuits to open once the rolling measure of errors exceeds this percent of requests
		ErrorPercentThreshold int `json:"error_percent_threshold"`

		// Interval which error percentage is calculated
		Interval time.Duration `json:"interval"`
	}
	// Breaker object
	Breaker struct {
		redisPool *redis.Pool

		interval              time.Duration
		requestCountThreshold int
		errorPercentThreshold int
		sleepWindow           time.Duration
	}
)

const (
	circuitStateClosed     = "CLOSED"
	circuitStateOpened     = "OPENED"
	circuitStateHalfOpened = "HALF_OPENED"
)

// NewBreakerWithCustomSettings instantiate a new breaker with custom settings
func NewBreakerWithCustomSettings(redisPool *redis.Pool, settings CircuitSettings) *Breaker {
	return &Breaker{
		redisPool:             redisPool,
		interval:              settings.Interval,
		requestCountThreshold: settings.RequestCountThreshold,
		errorPercentThreshold: settings.ErrorPercentThreshold,
		sleepWindow:           settings.SleepWindow,
	}
}

// NewBreaker with default settings
func NewBreaker(redisPool *redis.Pool) *Breaker {
	var defaultCircuitSettings = CircuitSettings{
		RequestCountThreshold: 10,
		SleepWindow:           2 * time.Second,
		ErrorPercentThreshold: 70,
		Interval:              3 * time.Second,
	}

	return &Breaker{
		redisPool:             redisPool,
		interval:              defaultCircuitSettings.Interval,
		requestCountThreshold: defaultCircuitSettings.RequestCountThreshold,
		errorPercentThreshold: defaultCircuitSettings.ErrorPercentThreshold,
		sleepWindow:           defaultCircuitSettings.SleepWindow,
	}
}

// Do invoke function with breaker without context
func (b Breaker) Do(name string, runFn func() error) error {
	runFnCtx := func(ctx context.Context) error {
		return runFn()
	}

	return b.DoCtx(context.Background(), name, runFnCtx)
}

// DoCtx invoke function with breaker with context
func (b Breaker) DoCtx(ctx context.Context, name string, runFnCtx func(ctx context.Context) error) error {
	state, osTTL, err := b.init(name)
	if err != nil {
		return fmt.Errorf("restrix: %s", err.Error())
	}
	if state == circuitStateOpened {
		if osTTL > 0 {
			return ErrCircuitOpened
		}
		state = circuitStateHalfOpened
	}

	reqCount, errCount, err := b.preRun(name)
	if err != nil {
		return fmt.Errorf("restrix: %s", err.Error())
	}

	err = runFnCtx(ctx)
	if err == nil {
		if state != circuitStateHalfOpened {
			// nothing to do here if current state is Closed
			return nil
		}

		// if current state is half opened, then switch state to close
		err = b.flipClose(name)
		if err != nil {
			fmt.Printf("restrix: %s", err.Error())
		}
		return nil
	}

	var errPercentage int
	if state == circuitStateHalfOpened {
		// reset osTTL if half opened
		goto FlipOpen
	}

	errPercentage = int((float64(errCount + 1)) / float64(reqCount) * 100)
	if reqCount < b.requestCountThreshold || errPercentage < b.errorPercentThreshold {
		if e := b.recordError(name); e != nil {
			fmt.Printf("restrix: %s", e.Error())
		}
		return err
	}

FlipOpen:
	if e := b.flipOpen(name); e != nil {
		fmt.Printf("restrix: %s", err.Error())
	}
	return err
}

func (b Breaker) init(name string) (state string, openStateTTL time.Duration, err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("SETNX", ns.currentState(), circuitStateClosed)
	if err != nil {
		return
	}
	err = conn.Send("GET", ns.currentState())
	if err != nil {
		return
	}
	err = conn.Send("GET", ns.openStateTTL())
	if err != nil {
		return
	}
	res, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return
	}

	cstate := string(res[1].([]byte))
	osTTL, _ := redis.Int(res[2], nil)
	return cstate, time.Duration(osTTL) * time.Second, err
}

func (b Breaker) preRun(name string) (reqCount, errCount int, err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("INCR", ns.requestCount())
	if err != nil {
		return
	}
	err = conn.Send("SETNX", ns.errorCount(), 0)
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.requestCount(), b.interval.Seconds(), "NX")
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.errorCount(), b.interval.Seconds(), "NX")
	if err != nil {
		return
	}
	err = conn.Send("GET", ns.requestCount())
	if err != nil {
		return
	}
	err = conn.Send("GET", ns.errorCount())
	if err != nil {
		return
	}

	res, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return
	}

	reqCount, _ = redis.Int(res[4], nil)
	errCount, _ = redis.Int(res[5], nil)

	return reqCount, errCount, nil
}

func (b Breaker) flipClose(name string) (err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("SET", ns.currentState(), circuitStateClosed)
	if err != nil {
		return
	}
	err = conn.Send("UNLINK", ns.openStateTTL())
	if err != nil {
		return
	}
	err = conn.Send("UNLINK", ns.requestCount())
	if err != nil {
		return
	}
	err = conn.Send("UNLINK", ns.errorCount())
	if err != nil {
		return
	}
	_, err = conn.Do("EXEC")

	return
}

func (b Breaker) flipOpen(name string) (err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("SET", ns.currentState(), circuitStateOpened)
	if err != nil {
		return
	}
	err = conn.Send("SET", ns.openStateTTL(), 1)
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.openStateTTL(), b.sleepWindow.Seconds(), "NX")
	if err != nil {
		return
	}
	err = conn.Send("SET", ns.lastOpenedAt(), time.Now().UTC().Unix())
	if err != nil {
		return
	}
	_, err = conn.Do("EXEC")

	return
}

func (b Breaker) recordError(name string) (err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("SETNX", ns.requestCount(), 1)
	if err != nil {
		return
	}
	err = conn.Send("SETNX", ns.errorCount(), 0)
	if err != nil {
		return
	}
	err = conn.Send("INCR", ns.errorCount())
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.requestCount(), b.interval.Seconds(), "NX")
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.errorCount(), b.interval.Seconds(), "NX")
	if err != nil {
		return
	}
	_, err = conn.Do("EXEC")

	return
}
