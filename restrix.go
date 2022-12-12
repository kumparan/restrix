package restrix

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

// CircuitSetting is used to tune circuit settings at runtime
type CircuitSetting struct {
	// RequestCountThreshold is the minimum number of requests needed before a circuit can be tripped due to health
	RequestCountThreshold int `json:"request_volume_threshold"`

	// SleepWindow is how long to wait after a circuit opens before testing for recovery
	SleepWindow time.Duration `json:"sleep_window"`

	// ErrorPercentThreshold causes circuits to open once the rolling measure of errors exceeds this percent of requests
	ErrorPercentThreshold int `json:"error_percent_threshold"`

	// Interval which error percentage is calculated
	Interval time.Duration `json:"interval"`
}

// Breaker object
type Breaker struct {
	redisPool redis.Pool

	interval              time.Duration
	requestCountThreshold int
	errorPercentThreshold int
	sleepWindow           time.Duration
}

func NewBreaker(redisPool redis.Pool, settings CircuitSetting) Breaker {
	return Breaker{
		redisPool:             redisPool,
		interval:              settings.Interval,
		requestCountThreshold: settings.RequestCountThreshold,
		errorPercentThreshold: settings.ErrorPercentThreshold,
		sleepWindow:           settings.SleepWindow,
	}
}

type CircuitState string

const (
	CircuitStateClosed     = "CLOSED"
	CircuitStateOpened     = "OPENED"
	CircuitStateHalfOpened = "HALF_OPENED"
)

// Do invoke function with breaker
func (b Breaker) Do(ctx context.Context, name string, runFn func(ctx context.Context) error) error {
	state, osTTL, err := b.init(name)
	if err != nil {
		return fmt.Errorf("restrix: %s", err.Error())
	}
	if state == CircuitStateOpened {
		if osTTL > 0 {
			return errors.New("restrix: circuit opened")
		}
		state = CircuitStateHalfOpened
	}

	reqCount, errCount, err := b.preRun(name)
	if err != nil {
		return fmt.Errorf("restrix: %s", err.Error())
	}

	err = runFn(ctx)
	if err == nil {
		if state != CircuitStateHalfOpened {
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
	if state == CircuitStateHalfOpened {
		// reset osTTL if half opened
		goto FlipOpen
	}

	errPercentage = errCount + 1/reqCount*100
	if reqCount < b.requestCountThreshold && errPercentage < b.errorPercentThreshold {
		err = b.recordError(name)
		if err != nil {
			fmt.Printf("restrix: %s", err.Error())
		}
		return nil
	}

FlipOpen:
	err = b.flipOpen(name)
	if err != nil {
		fmt.Printf("restrix: %s", err.Error())
	}
	return nil
}

func (b Breaker) init(name string) (state CircuitState, openStateTTL time.Duration, err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("SETNX", ns.currentState(), CircuitStateClosed)
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
	res, err := redis.Strings(conn.Do("EXEC"))
	if err != nil {
		return
	}

	osTTL, _ := strconv.Atoi(res[2])
	return CircuitState(res[1]), time.Duration(osTTL) * time.Second, err
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
	err = conn.Send("EXPIRE", ns.requestCount(), b.interval, "NX")
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.errorCount(), b.interval, "NX")
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

	res, err := redis.Strings(conn.Do("EXEC"))
	if err != nil {
		return
	}

	reqCount, _ = strconv.Atoi(res[4])
	errCount, _ = strconv.Atoi(res[5])

	return reqCount, errCount, nil
}

func (b Breaker) flipClose(name string) (err error) {
	ns := newNamespacer(name)

	conn := b.redisPool.Get()
	err = conn.Send("MULTI")
	if err != nil {
		return
	}
	err = conn.Send("SET", ns.currentState(), CircuitStateClosed)
	if err != nil {
		return
	}
	err = conn.Send("UNLINK", ns.openStateTTL())
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
	err = conn.Send("SET", ns.currentState(), CircuitStateOpened)
	if err != nil {
		return
	}
	err = conn.Send("SET", ns.openStateTTL(), 1, b.sleepWindow)
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.openStateTTL(), b.sleepWindow, "NX")
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
	err = conn.Send("INCR", ns.errorCount())
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.requestCount(), b.interval, "NX")
	if err != nil {
		return
	}
	err = conn.Send("EXPIRE", ns.errorCount(), b.interval, "NX")
	if err != nil {
		return
	}
	_, err = conn.Do("EXEC")

	return
}
