package rm_utils

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sweemingdow/rabbitmq-go-client-pro/config"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	once       sync.Once
	addressSli []string
	addrLen    int
	addrErr    error
)

func CreateFloorConn(cfg rm_cfg.RabbitmqCfg) (*amqp091.Connection, error) {
	once.Do(func() {
		if len(cfg.Addresses) == 0 {
			addrErr = errors.New("can not create connection without addresses:" + cfg.Addresses)
			return
		}

		addressSli = strings.Split(cfg.Addresses, ",")
		addrLen = len(addressSli)
	})

	if addrErr != nil {
		return nil, addrErr
	}

	var hp string
	if addrLen == 0 {
		return nil, errors.New("can not create connection with empty addresses")
	} else if addrLen == 1 {
		hp = addressSli[0]
	} else {
		idx := rand.Intn(addrLen)
		hp = addressSli[idx]
	}

	virtualHost := cfg.VirtualHost
	if virtualHost == "" {
		virtualHost = "/"
	}

	dsn := fmt.Sprintf("amqp://%s:%s@%s%s", cfg.Username, cfg.Password, hp, virtualHost)
	return amqp091.DialConfig(dsn, amqp091.Config{})
}

func CalcRetryInterval(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	delay := baseDelay * time.Duration(1<<attempt)
	if delay > maxDelay {
		return maxDelay
	}

	half := delay / 2
	if half <= 0 {
		return 0
	}

	delay = half + time.Duration(rand.Int63n(int64(half)))

	return delay
}

func CalcExecAvaTime(ctx context.Context, min, def time.Duration) time.Duration {
	var timeout = def
	if dl, ok := ctx.Deadline(); ok {
		if remainTime := time.Until(dl); remainTime > 0 {
			if adjustedTimeout := remainTime / 2; adjustedTimeout < timeout {
				timeout = adjustedTimeout
			}
			if timeout < min {
				timeout = min
			}
		}
	}

	return timeout
}

type TimerPool interface {
	Get(time.Duration) *time.Timer

	Put(*time.Timer)
}

type timerPool struct {
	timerPool sync.Pool
}

func (tp *timerPool) Get(timeout time.Duration) *time.Timer {
	t := tp.timerPool.Get().(*time.Timer)
	t.Reset(timeout)
	return t
}

func (tp *timerPool) Put(t *time.Timer) {
	t.Stop()
	tp.timerPool.Put(t)
}

func NewTimerPool() TimerPool {
	return &timerPool{
		timerPool: sync.Pool{
			New: func() interface{} {
				t := time.NewTimer(time.Hour)
				t.Stop()
				return t
			},
		},
	}
}

func CASUpdateUi32Min(min *atomic.Uint32, newVal uint32) {
	for {
		curr := min.Load()
		if newVal >= curr {
			return
		}

		if min.CompareAndSwap(curr, newVal) {
			return
		}
	}
}

func CASUpdateUi32Max(max *atomic.Uint32, newVal uint32) {
	for {
		curr := max.Load()
		if newVal <= curr {
			return
		}

		if max.CompareAndSwap(curr, newVal) {
			return
		}
	}
}
