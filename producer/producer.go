package rm_producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/alphadose/haxmap"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sweemingdow/rabbitmq-go-client-pro/config"
	"github.com/sweemingdow/rabbitmq-go-client-pro/log"
	"github.com/sweemingdow/rabbitmq-go-client-pro/utils"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	reconnectMaxDelayForConn = 60 * time.Second
	waitingMagicNum          = 8
	waitingConfirmDone       = 5
)

var (
	ProducerCliAlreadyClosed = errors.New("producer client was closed")
	ProducerConnInvalidErr   = errors.New("producer connection was invalid")
	ProducerConnSuspendErr   = errors.New("producer connection was suspend")
	ProducerChInvalidErr     = errors.New("producer channel was invalid")
	ProducerChWasCleaned     = errors.New("producer channel was cleaned")
	AcquireChannelTimeoutErr = errors.New("acquire producer channel timeout")
)

var (
	confirmCb ConfirmCallback
	returnCb  ReturnCallback
)

type producerConn struct {
	id      string // eg: p-conn-{no.}
	idx     int    // index
	closed  atomic.Bool
	dirty   atomic.Bool
	conn    *amqp091.Connection // real floor connection
	chMu    sync.Mutex
	chPool  chan *ProducerChan // all channels under this connection
	poolLen uint64
	ctime   time.Time
	chIncr  atomic.Uint32
	trying  atomic.Bool
	pc      *ProducerClient

	lastErr         atomic.Pointer[amqp091.Error]
	errTime         atomic.Pointer[time.Time]
	suspendDeadline atomic.Pointer[time.Time]
}

type ProducerChan struct {
	id     string           // eg: p-conn-{no.}-ch-{no.}
	ch     *amqp091.Channel // real floor channel
	closed atomic.Bool
	dirty  atomic.Bool
	ctime  time.Time
	conn   *producerConn
}

type ProducerClient struct {
	conns           []*producerConn // all connections in client
	connMu          sync.RWMutex
	connsLen        uint64
	cfg             rm_cfg.RabbitmqCfg
	connIncr        atomic.Uint32
	closed          atomic.Bool
	done            chan struct{}
	timerPool       rm_utils.TimerPool
	chooseIncr      atomic.Uint64
	enableConfirm   bool // if true start ack/nack confirm callback
	enableMandatory bool // if true start return callback
	deliverId2msgId *haxmap.Map[string, string]
}

func NewProducerClient(cfg rm_cfg.RabbitmqCfg) (*ProducerClient, error) {
	if cfg.ProducerCfg.MaxConnections <= 0 {
		rm_log.Fatal(fmt.Sprintf("max connections is invalid"))
	}

	if cfg.ProducerCfg.MaxChannelsPerConn <= 0 {
		rm_log.Fatal(fmt.Sprintf("max channels is invalid"))
	}

	cli := &ProducerClient{
		cfg:             cfg,
		connsLen:        uint64(cfg.ProducerCfg.MaxConnections),
		timerPool:       rm_utils.NewTimerPool(),
		done:            make(chan struct{}),
		enableConfirm:   cfg.ProducerCfg.EnableConfirm,
		enableMandatory: cfg.ProducerCfg.EnableMandatory,
	}

	conns, err := cli.mustInitConns()
	if err != nil {
		return nil, err
	}

	cli.conns = conns
	cli.enableConfirm = cfg.ProducerCfg.EnableConfirm
	if cfg.ProducerCfg.EnableConfirm {
		cli.deliverId2msgId = haxmap.New[string, string](100)
	}
	cli.enableMandatory = cfg.ProducerCfg.EnableMandatory

	setEnableStats(cfg.ProducerCfg.EnableStats)

	if rm_log.CanDebug() {
		rm_log.Debug("rabbitmq producer client init successfully")
	}

	rm_log.SetLoggerLevelHuman(cfg.LogLevel)

	return cli, nil
}

func (pc *ProducerClient) mustInitConns() (conns []*producerConn, err error) {
	connCnt := pc.cfg.ProducerCfg.MaxConnections
	conns = make([]*producerConn, connCnt)

	// clear if error
	defer func() {
		if err != nil {
			for i := 0; i < connCnt; i++ {
				if conns[i] != nil {
					_ = conns[i].close()
				}
			}
		}
	}()

	for i := 0; i < connCnt; i++ {
		floorConn, ce := rm_utils.CreateFloorConn(pc.cfg)
		if ce != nil {
			err = ce
			return
		}
		conn := pc.createProducerConn(i, floorConn)
		conns[i] = conn

		// listen connection event
		go conn.listenEvent()
	}

	for _, conn := range conns {
		conn.chPool = make(chan *ProducerChan, pc.cfg.ProducerCfg.MaxChannelsPerConn)
		for j := 0; j < pc.cfg.ProducerCfg.MaxChannelsPerConn; j++ {
			childCh, ce := conn.createProducerChan()
			if ce != nil {
				err = ce
				return
			}

			conn.chPool <- childCh

			// listen channel event
			go childCh.listenEvent()
		}
	}

	return conns, err
}

func (cn *producerConn) isSuspend() bool {
	if sdl := cn.suspendDeadline.Load(); sdl != nil {
		return time.Now().Before(*sdl)
	}

	return false
}

func (cn *producerConn) markSuspend(amqpErr *amqp091.Error, dur time.Duration) {
	now := time.Now()

	cn.chMu.Lock()

	cn.lastErr.Store(amqpErr)
	cn.errTime.Store(&now)
	deadline := now.Add(dur)
	cn.suspendDeadline.Store(&deadline)

	cn.chMu.Unlock()

	// evict invalid channels after dur
	go cn.tryEvict(dur)
}

func (cn *producerConn) markUnsuspend() {
	cn.chMu.Lock()

	cn.lastErr.Store(nil)
	cn.errTime.Store(nil)
	cn.suspendDeadline.Store(nil)

	cn.chMu.Unlock()
}

func (cn *producerConn) tryEvict(dur time.Duration) {
	rm_log.Warn(fmt.Sprintf("conn:%s will be evict invalid channels after %v sec later", cn.id, dur.Seconds()))

	recTime := time.NewTimer(dur)
	defer recTime.Stop()

	for {
		select {
		case <-cn.pc.done:
			return
		case <-recTime.C:
			rm_log.Info(fmt.Sprintf("conn:%s start evict invalid channels", cn.id))

			cn.cleanAndRefillPool()
		}
	}
}

func (cn *producerConn) cleanAndRefillPool() {
	cn.chMu.Lock()
	defer cn.chMu.Unlock()

	if cn.isDirty() {
		return
	}

	cleaned := 0
	for {
		select {
		case ch := <-cn.chPool:
			if !ch.valid() {
				_ = ch.close()
				cleaned++
			} else {
				select {
				case cn.chPool <- ch:
				default:
					_ = ch.close()
				}
			}
		default:
			// 池已空，跳出循环
			goto refill
		}
	}

refill:
	if cleaned > 0 {
		rm_log.Warn(fmt.Sprintf("conn:%s, had %d invalid channels should be evict", cn.id, cleaned))
	}

	targetCount := cn.pc.cfg.ProducerCfg.MaxChannelsPerConn
	currentCount := len(cn.chPool)

	var recErr error
	for i := 0; i < targetCount-currentCount; i++ {
		newCh, err := cn.createProducerChan()
		if err != nil {
			rm_log.Error(fmt.Sprintf("create channel failed when conn:%s evict invalid channels, err:%v", cn.id, err))
			recErr = err
			break
		}

		select {
		case cn.chPool <- newCh:
			go newCh.listenEvent()
		default:
			_ = newCh.close()
		}
	}

	cn.markUnsuspend()

	if recErr == nil {
		rm_log.Info(fmt.Sprintf("conn:%s evict invalid channels successfully", cn.id))
	}
}

func (cn *producerConn) listenEvent() {
	connErrCh := cn.conn.NotifyClose(make(chan *amqp091.Error, 1))

	for {
		select {
		case <-cn.pc.done:
			// client was closed
			return
		case closeErr, ok := <-connErrCh:
			if !ok {
				// error channel was closed
				return
			}

			// mark as dirty
			cn.markDirty()

			if closeErr != nil {
				rm_log.Error(fmt.Sprintf("receive producer connection closed event, reason:%v", closeErr))

				var (
					pc     = cn.pc
					oldIdx = cn.idx
				)

				cn.chMu.Lock()
				_ = cn.close()
				cn.chMu.Unlock()

				var (
					baseDelay = time.Duration(cn.pc.cfg.ProducerCfg.ConnRetryDelayMills) * time.Millisecond
					maxDelay  = reconnectMaxDelayForConn
				)

				// reconnect util successfully
				newConn, err := cn.pc.reconnectUtilSuccess(oldIdx, baseDelay, maxDelay)

				if err == nil && newConn != nil {
					// retry success, replace connection
					pc.connMu.Lock()
					pc.conns[oldIdx] = newConn
					pc.connMu.Unlock()

					rm_log.Info(fmt.Sprintf("recreate producer connection success after receive close event, newConn:%s", cn.id))

					// listen connection event
					go newConn.listenEvent()
				}
			}

			// exit this goroutine
			return
		}
	}
}

func (pc *ProducerClient) reconnectUtilSuccess(oldIdx int, baseDelay, maxDelay time.Duration) (*producerConn, error) {
	attempt := 0
	for {
		select {
		case <-pc.done:
			return nil, fmt.Errorf("reconnect producer connection, recevie done event, exit retry")
		default:
			attempt++

			val, err := recreateProducerConn(oldIdx, pc)
			if err == nil {
				rm_log.Info(fmt.Sprintf("retry reconnect producer connection success at attempt:%d", attempt))
				return val, nil
			}

			rm_log.Warn(fmt.Sprintf("retry reconnect producer connection failed at attempt:%d", attempt))

			backoff := rm_utils.CalcRetryInterval(attempt, baseDelay, maxDelay)

			if rm_log.CanDebug() {
				rm_log.Debug(fmt.Sprintf("retry reconnect producer connection attempt:%d, sleeping for:%v before next retry", attempt, backoff))
			}

			time.Sleep(backoff)
		}
	}
}

func (ch *ProducerChan) listenEvent() {
	channelErrorCh := ch.ch.NotifyClose(make(chan *amqp091.Error, 1))

	var confirmCap = 1
	if ch.conn.pc.enableConfirm {
		confirmCap = ch.conn.pc.cfg.ProducerCfg.ConfirmQueueCap
		if confirmCap <= 0 {
			confirmCap = 100
		}
	}

	confirmCh := ch.ch.NotifyPublish(make(chan amqp091.Confirmation, confirmCap))
	returnCh := ch.ch.NotifyReturn(make(chan amqp091.Return, 1))

	for {
		select {
		case <-ch.conn.pc.done:
			return
		case cof, ok := <-confirmCh:
			if ok && ch.conn.pc.enableConfirm && confirmCb != nil {
				deliverId := fmt.Sprintf("%s-%d", ch.id, cof.DeliveryTag)
				if msgId, exists := ch.conn.pc.deliverId2msgId.GetAndDel(deliverId); exists {
					confirmCb(msgId, cof.DeliveryTag, cof.Ack)
				}
			}
		case ret, ok := <-returnCh:
			if ok && ch.conn.pc.enableMandatory && returnCb != nil {
				returnCb(ret)
			}
		case closeErr, ok := <-channelErrorCh:
			if !ok {
				return
			}

			// mark as dirty
			ch.markDirty()

			if closeErr != nil {
				rm_log.Error(fmt.Sprintf("receive producer channel:%s closed event, reason:%v", ch.id, closeErr))

				_ = ch.close()

				cn := ch.conn

				// connection was died
				if cn.isDirty() {
					return
				}

				// 320: forced closed
				if closeErr.Code == amqp091.ConnectionForced {
					return
				}

				// Unrecoverable error, suspend the entire connection until automatic recovery is attempted after configuring: "RecoverMillsAfterSuspend"
				if isUnrecoverableError(closeErr) {
					mills := cn.pc.cfg.ProducerCfg.RecoverMillsAfterSuspend
					if mills <= 0 {
						mills = 10 * 1000
					}
					cn.markSuspend(closeErr, time.Duration(mills)*time.Millisecond)
					return
				}

				// channel closed, but connection is ok, recreate channel
				newCh, err := cn.createProducerChan()

				if err != nil {
					rm_log.Error("recreate producer channel failed after receive close event")
					return
				}

				// check again
				if cn.isDirty() {
					_ = newCh.close()
					return
				}

				// put into the pool
				if cn.pc.PutProduceCh(newCh) {
					rm_log.Info(fmt.Sprintf("recreate producer channel success after receive close event, conn:%s, channel:%s", cn.id, newCh.id))

					// listen channel event
					go newCh.listenEvent()
				}
			}

			// exit this goroutine
			return
		}
	}
}

func (ch *ProducerChan) close() error {
	if !ch.closed.CompareAndSwap(false, true) {
		return nil
	}

	if ch.ch != nil {
		// It is safe to call this method multiple times.
		err := ch.ch.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (pc *ProducerClient) PutProduceCh(ch *ProducerChan) bool {
	if pc.closed.Load() {
		return false
	}

	if !ch.valid() {
		_ = ch.close()
		return false
	} else {
		conn := ch.conn
		if conn.isDirty() {
			_ = ch.close()
			return false
		}
	}

	conn := ch.conn
	select {
	case conn.chPool <- ch:
		return true
	default:
		rm_log.Warn(fmt.Sprintf("producer connection:%s channels pool was fully, discard this channel:%s", conn.id, ch.id))
		_ = ch.close()
		return false
	}
}

func (pc *ProducerClient) GetProduceCh(timeout time.Duration) (*ProducerChan, error) {
	if pc.closed.Load() {
		return nil, ProducerCliAlreadyClosed
	}

	conn := pc.chooseConn()

	if conn.valid() {
		return conn.acquireChan(timeout)
	} else {
		if rm_log.CanDebug() {
			rm_log.Debug(fmt.Sprintf("choose connection, conn:%s was invalid, choose connection again+1", conn.id))
		}

		// choose again right now
		conn = pc.chooseConn()

		if conn.valid() {
			return conn.acquireChan(timeout)
		} else {
			// connection is still unavailable, get it again after a short sleep
			// the connection may be quickly restored at this time
			var waitMills = waitingMagicNum * time.Millisecond
			time.Sleep(waitMills)

			if rm_log.CanDebug() {
				rm_log.Debug(fmt.Sprintf("choose connection, conn:%swas invalid, choose connection again+2 after:%v", conn.id, waitMills))
			}

			conn = pc.chooseConn()

			remaining := timeout - waitMills

			if conn.valid() {
				return conn.acquireChan(remaining)
			} else {
				// waiting util timeout
				timer := pc.timerPool.Get(remaining)
				defer pc.timerPool.Put(timer)

				start := time.Now()
				attempt := 0
				for {
					select {
					case <-timer.C:
						return nil, AcquireChannelTimeoutErr
					default:
						attempt++

						time.Sleep(rm_utils.CalcRetryInterval(attempt, waitMills, remaining))

						if rm_log.CanTrace() {
							rm_log.Trace(fmt.Sprintf("choose connection, conn:%s was invalid, choose connection again+%d after:%v", conn.id, attempt+2, time.Since(start)))
						}

						conn = pc.chooseConn()

						if conn.valid() {
							leaveTime := remaining - time.Since(start)
							if leaveTime <= 0 {
								return nil, AcquireChannelTimeoutErr
							}

							return conn.acquireChan(leaveTime)
						}

						leaveTime := remaining - time.Since(start)
						if leaveTime <= 0 {
							return nil, AcquireChannelTimeoutErr
						}
					}
				}
			}
		}
	}
}

func (pc *ProducerClient) Shutdown(ctx context.Context) error {
	if !pc.closed.CompareAndSwap(false, true) {
		return nil
	}

	rm_log.Info("producer receive shutdown cmd, will waiting unconfirmed callbacks complete")

	pc.waitingConfirmCallbackDone(ctx)

	close(pc.done)

	pc.connMu.Lock()
	defer pc.connMu.Unlock()

	var last error
	if len(pc.conns) > 0 {
		for _, conn := range pc.conns {
			conn.markDirty()
			last = conn.close()
		}
	}

	if last == nil {
		rm_log.Info(fmt.Sprintf("rabbitmq producer client shutdown successfully"))
	}

	return last
}

func (pc *ProducerClient) waitingConfirmCallbackDone(ctx context.Context) {
	if !pc.enableConfirm {
		return
	}

	if unconfirmedLen := pc.deliverId2msgId.Len(); unconfirmedLen <= 0 {
		return
	} else {
		rm_log.Warn(fmt.Sprintf("there are still unconfirmed messages, when shutdown, unconfirmed count:%d", unconfirmedLen))

		confirmDone := make(chan struct{})
		go func() {
			var timeout = rm_utils.CalcExecAvaTime(ctx, 100*time.Millisecond, waitingConfirmDone*time.Second)

			timer := time.NewTimer(timeout)
			defer timer.Stop()

			for pc.deliverId2msgId.Len() > 0 {
				select {
				case <-ctx.Done():
					rm_log.Warn(fmt.Sprintf("waiting for all confirm callback, be canceled:%v", ctx.Err()))
					return
				case <-timer.C:
					rm_log.Warn(fmt.Sprintf("waiting for all confirm callback, timeout after:%d seconds", waitingConfirmDone))
					return
				default:

				}

				time.Sleep(3 * time.Millisecond)
			}

			close(confirmDone)
		}()

		select {
		case <-confirmDone:
			rm_log.Info(fmt.Sprintf("all unconfirmed callback handle completed"))
		case <-ctx.Done():
		}
	}

}

func (ch *ProducerChan) String() string {
	return fmt.Sprintf("id:%s, ctime:%v", ch.id, ch.ctime)
}

func (pc *ProducerClient) createProducerConn(idx int, conn *amqp091.Connection) *producerConn {
	return &producerConn{
		id:      fmt.Sprintf("p-conn-%d", pc.connIncr.Add(1)),
		idx:     idx,
		conn:    conn,
		poolLen: uint64(pc.cfg.ProducerCfg.MaxChannelsPerConn),
		ctime:   time.Now(),
		pc:      pc,
	}
}

func (pc *ProducerClient) chooseConn() *producerConn {
	pc.connMu.RLock()
	defer pc.connMu.RUnlock()

	var idx int
	if pc.connsLen == 1 {
		idx = 0
	} else {
		idx = int(pc.chooseIncr.Add(1) % pc.connsLen)
	}

	conn := pc.conns[idx]

	return conn
}

func (cn *producerConn) isDirty() bool {
	return cn.dirty.Load()
}

func (cn *producerConn) close() error {
	if !cn.closed.CompareAndSwap(false, true) {
		return nil
	}

	if cn.chPool != nil {
		close(cn.chPool)

		for ch := range cn.chPool {
			if ch == nil {
				continue
			}

			// mark dirty firstly
			ch.markDirty()
			_ = ch.close()
		}
	}

	if !cn.conn.IsClosed() {
		return cn.conn.Close()
	}

	return nil
}

func (cn *producerConn) valid() bool {
	return !cn.isDirty() &&
		cn.conn != nil &&
		!cn.conn.IsClosed()
}

func (cn *producerConn) acquireChan(timeout time.Duration) (*ProducerChan, error) {
	if cn.isDirty() {
		if rm_log.CanTrace() {
			rm_log.Trace(fmt.Sprintf("acquire channel, but conn:%s was dirty", cn.id))
		}

		return nil, ProducerConnInvalidErr
	}

	if cn.isSuspend() {
		if rm_log.CanTrace() {
			rm_log.Trace(fmt.Sprintf("acquire channel, but conn:%s was suspend, cause:%v", cn.id, cn.lastErr.Load()))
		}

		return nil, ProducerConnSuspendErr
	}

	timer := cn.pc.timerPool.Get(timeout)
	defer cn.pc.timerPool.Put(timer)

	start := time.Now()
	select {
	case <-timer.C:
		return nil, AcquireChannelTimeoutErr
	case ch, ok := <-cn.chPool:
		if !ok {
			return nil, ProducerChWasCleaned
		}

		if ch.valid() {
			return ch, nil
		} else {
			if rm_log.CanTrace() {
				rm_log.Trace(fmt.Sprintf("acquired channel, but invalid, conn:%s, chan:%s", cn.id, ch.id))
			}

			if cn.isDirty() {
				return nil, ProducerConnInvalidErr
			}

			// Return it to the next caller
			select {
			case cn.chPool <- ch:
			default:

			}
		}
	}

	select {
	case <-timer.C:
		return nil, AcquireChannelTimeoutErr
	case ch, ok := <-cn.chPool:
		if !ok {
			return nil, ProducerChWasCleaned
		} else {
			if ch.valid() {
				return ch, nil
			} else {
				if rm_log.CanTrace() {
					rm_log.Trace(fmt.Sprintf("acquired channel again but still invalid, will recreate channel, conn:%s, chan:%s", cn.id, ch.id))
				}

				if cn.isDirty() {
					return nil, ProducerConnInvalidErr
				}

				for !cn.trying.CompareAndSwap(false, true) {
					runtime.Gosched()
				}
				defer cn.trying.Store(false)

				if time.Since(start) >= timeout {
					return nil, AcquireChannelTimeoutErr
				}

				// try to create new channel
				newCh, err := cn.createProducerChan()
				if err != nil {
					return nil, err
				}

				if smp.enableStats {
					smp.recreateChCntAfterWaiting.Add(1)
				}

				if rm_log.CanTrace() {
					rm_log.Trace(fmt.Sprintf("recreate channel success, conn:%s, chan:%s", cn.id, ch.id))
				}

				// push into the pool
				if cn.pc.PutProduceCh(newCh) {
					if rm_log.CanTrace() {
						rm_log.Trace(fmt.Sprintf("recreated channel and push into the pool success, conn:%s, chan:%s", cn.id, ch.id))
					}

					go newCh.listenEvent()
				}

				return newCh, nil
			}
		}
	}
}

func (cn *producerConn) markDirty() {
	cn.dirty.Store(true)
}

func (cn *producerConn) createChildChannels() error {
	cn.chPool = make(chan *ProducerChan, cn.pc.cfg.ProducerCfg.MaxChannelsPerConn)

	for j := 0; j < cn.pc.cfg.ProducerCfg.MaxChannelsPerConn; j++ {
		childCh, ce := cn.createProducerChan()
		if ce != nil {
			return ce
		}

		cn.chPool <- childCh

		go childCh.listenEvent()
	}

	return nil
}

func (cn *producerConn) createProducerChan() (*ProducerChan, error) {
	if !cn.valid() {
		return nil, ProducerConnInvalidErr
	}

	ch, err := cn.conn.Channel()
	if err != nil {
		return nil, err
	}

	if cn.isDirty() {
		_ = ch.Close()
		return nil, ProducerConnInvalidErr
	}

	if cn.pc.enableConfirm {
		if err = ch.Confirm(false); err != nil {
			return nil, err
		}
	}

	return &ProducerChan{
		id:    fmt.Sprintf("%s-%d", cn.id, cn.chIncr.Add(1)),
		ch:    ch,
		ctime: time.Now(),
		conn:  cn,
	}, nil
}

func (ch *ProducerChan) markDirty() {
	ch.dirty.Store(true)
}

func (ch *ProducerChan) isDirty() bool {
	return ch.dirty.Load() || ch.conn.isDirty()
}

func (ch *ProducerChan) valid() bool {
	return !ch.isDirty() &&
		ch.ch != nil &&
		!ch.ch.IsClosed()
}

func (ch *ProducerChan) GetCh() *amqp091.Channel {
	return ch.ch
}

func recreateProducerConn(oldIdx int, pc *ProducerClient) (*producerConn, error) {
	floorConn, ce := rm_utils.CreateFloorConn(pc.cfg)
	if ce != nil {
		return nil, ce
	}

	conn := pc.createProducerConn(oldIdx, floorConn)

	return conn, conn.createChildChannels()
}

func isUnrecoverableError(amqpErr *amqp091.Error) bool {
	switch amqpErr.Code {
	case amqp091.NotFound, amqp091.AccessRefused, amqp091.PreconditionFailed, amqp091.FrameError, amqp091.SyntaxError:
		return true
	}

	return false
}

const (
	defaultTimeout = 4 * time.Second
)

type UseFunc func(ch *amqp091.Channel) error

type ConfirmFunc func(msgId string, ch *amqp091.Channel) error

// need a msgId to relation the msg
// only deliverTag nothing to do ...
type ConfirmCallback func(msgId string, deliverTag uint64, isAck bool)

type ReturnCallback func(ret amqp091.Return)

func (pc *ProducerClient) withChannel(timeout time.Duration, run func(ch *ProducerChan) error) error {
	if smp.enableStats {
		start := time.Now()
		rabbitCh, err := pc.GetProduceCh(timeout)
		if err != nil {
			return err
		}

		defer func() {
			putStart := time.Now()
			pc.PutProduceCh(rabbitCh)
			smp.updatePutStatsState(time.Since(putStart))
		}()

		smp.updateAcquireStatsState(time.Since(start))

		return run(rabbitCh)
	} else {
		rabbitCh, err := pc.GetProduceCh(timeout)
		if err != nil {
			return err
		}

		defer pc.PutProduceCh(rabbitCh)

		return run(rabbitCh)
	}
}

func (pc *ProducerClient) WithUse(timeout time.Duration, uf UseFunc) error {
	return pc.withChannel(timeout, func(ch *ProducerChan) error {
		return uf(ch.ch)
	})
}

func (pc *ProducerClient) WithDefaultUse(uf UseFunc) error {
	return pc.WithUse(defaultTimeout, uf)
}

func (pc *ProducerClient) WithConfirm(msgId string, timeout time.Duration, cf ConfirmFunc) error {

	return pc.withChannel(timeout, func(ch *ProducerChan) error {
		mid := handleForConfirm(msgId, pc, ch)
		return cf(mid, ch.ch)
	})
}

func (pc *ProducerClient) WithConfirmDefault(msgId string, cf ConfirmFunc) error {
	return pc.withChannel(defaultTimeout, func(ch *ProducerChan) error {
		mid := handleForConfirm(msgId, pc, ch)

		return cf(mid, ch.ch)
	})
}

// don't care msgId
func (pc *ProducerClient) JustConfirm(cf ConfirmFunc) error {
	return pc.withChannel(defaultTimeout, func(ch *ProducerChan) error {
		mid := handleForConfirm("", pc, ch)

		return cf(mid, ch.ch)
	})
}

func SetConfirmCallback(ccb ConfirmCallback) {
	confirmCb = ccb
}

func SetReturnCallback(rcb ReturnCallback) {
	returnCb = rcb
}

func createMsgId() string {
	return uuid.New().String()
}

func handleForConfirm(msgId string, pc *ProducerClient, ch *ProducerChan) string {
	nextTag := ch.ch.GetNextPublishSeqNo()
	if msgId == "" {
		msgId = createMsgId()
	}

	pc.deliverId2msgId.Set(fmt.Sprintf("%s-%d", ch.id, nextTag), msgId)

	return msgId
}
