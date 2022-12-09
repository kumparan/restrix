package restrix

import (
	"context"
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

type runFuncC func(context.Context) error

type CircuitState string

const (
	CircuitStateClosed = "CLOSED"
	CircuitStateOpened = "OPENED"
)

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

func (b Breaker) preRun(name string) (state CircuitState, reqCount, errCount int, err error) {
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
	err = conn.Send("GET", ns.currentState())
	if err != nil {
		return
	}

	res, err := redis.Strings(conn.Do("EXEC"))
	if err != nil {
		return
	}

	reqCount, _ = strconv.Atoi(res[4])
	errCount, _ = strconv.Atoi(res[5])

	return CircuitState(res[6]), reqCount, errCount, nil
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
