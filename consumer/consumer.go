package rm_consumer

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sweemingdow/rabbitmq-go-client-pro/config"
	"github.com/sweemingdow/rabbitmq-go-client-pro/log"
	"github.com/sweemingdow/rabbitmq-go-client-pro/utils"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrConsumerAlreadyClosed = errors.New("consumer client was closed")
	ErrConnectionInvalid     = errors.New("consumer connection was invalid")
)

var (
	reconnectMaxDelayForConn = 60 * time.Second
	waitingConsumeDoneDur    = 20 * time.Second
	recreateChannelMaxDelay  = 45 * time.Second
)

type ConsumerClient struct {
	conns      []*consumerConn // all connections in client
	connMu     sync.RWMutex
	cfg        rm_cfg.RabbitmqCfg
	connIncr   atomic.Uint32
	chooseIncr atomic.Uint64
	closed     atomic.Bool
	done       chan struct{}
	declared   atomic.Bool
	processWg  sync.WaitGroup
}

type consumerConn struct {
	id           string // eg: c-conn-{no.}
	idx          int
	closed       atomic.Bool
	dirty        atomic.Bool
	conn         *amqp091.Connection
	chMu         sync.Mutex
	chanChildren map[string]*consumerChan
	ctime        time.Time
	chIncr       atomic.Uint32
	cc           *ConsumerClient
	connClosed   chan struct{}
}

type consumerChan struct {
	id         string // eg: c-conn-{no.}-ch-{no.}
	ch         *amqp091.Channel
	ctime      time.Time
	conn       *consumerConn
	closed     atomic.Bool
	canceled   atomic.Bool
	willExitCh chan struct{}
}

// a hook to declare rabbitmq components after init success
type DeclareHook func(tempCh *amqp091.Channel) error

func NewConsumerClient(cfg rm_cfg.RabbitmqCfg, dh DeclareHook) (*ConsumerClient, error) {
	if cfg.ConsumerCfg.MaxConnections <= 0 {
		cfg.ConsumerCfg.MaxConnections = 1
	}

	cc := &ConsumerClient{
		cfg:  cfg,
		done: make(chan struct{}),
	}

	conns, err := cc.mustInitConns()
	if err != nil {
		return nil, err
	}

	cc.conns = conns

	if rm_log.CanDebug() {
		rm_log.Debug("rabbitmq consumer client init successfully")
	}

	rm_log.SetLoggerLevelHuman(cfg.LogLevel)

	if dh != nil {
		if err = cc.WithTempCh(func(ch *amqp091.Channel) error {
			return dh(ch)
		}); err != nil {
			return nil, err
		}
	}

	return cc, nil
}

func (cc *ConsumerClient) mustInitConns() ([]*consumerConn, error) {
	connCnt := cc.cfg.ConsumerCfg.MaxConnections
	conns := make([]*consumerConn, 0, connCnt)

	for i := 0; i < connCnt; i++ {
		floorConn, err := rm_utils.CreateFloorConn(cc.cfg)
		if err != nil {
			// clear already created connections
			for _, conn := range conns {
				_ = conn.close()
			}
			return nil, err
		}

		conn := cc.createConsumerConn(i, floorConn)
		conns = append(conns, conn)

		go conn.listenEvent()
	}

	return conns, nil
}

func (cc *ConsumerClient) createConsumerConn(idx int, conn *amqp091.Connection) *consumerConn {
	return &consumerConn{
		id:           fmt.Sprintf("c-conn-%d", cc.connIncr.Add(1)),
		idx:          idx,
		conn:         conn,
		ctime:        time.Now(),
		cc:           cc,
		chanChildren: make(map[string]*consumerChan),
		connClosed:   make(chan struct{}),
	}
}

func (cn *consumerConn) listenEvent() {
	connErrCh := cn.conn.NotifyClose(make(chan *amqp091.Error, 1))

	select {
	case <-cn.cc.done:
		return
	case closeErr, ok := <-connErrCh:
		if !ok {
			return
		}

		// mark dirty firstly
		cn.markDirty()

		if closeErr != nil {

			rm_log.Error(fmt.Sprintf("receive consumer connection closed event, reason:%v", closeErr))

			_ = cn.close()

			var (
				baseDelay = time.Duration(cn.cc.cfg.ConsumerCfg.ConnRetryDelayMills) * time.Millisecond
				maxDelay  = reconnectMaxDelayForConn
				cc        = cn.cc
				oldIdx    = cn.idx
			)

			// reconnect util successfully
			newConn, err := cn.cc.reconnectUtilSuccess(oldIdx, baseDelay, maxDelay)

			if err == nil && newConn != nil {
				cc.connMu.Lock()
				cc.conns[oldIdx] = newConn
				cc.connMu.Unlock()

				rm_log.Info(fmt.Sprintf("recreate consumer connection success after receive close event, newConn:%s", cn.id))

				go newConn.listenEvent()
			}
		}
	}
}

func (cc *ConsumerClient) reconnectUtilSuccess(oldIdx int, baseDelay, maxDelay time.Duration) (*consumerConn, error) {
	attempt := 0
	for {
		select {
		case <-cc.done:
			return nil, fmt.Errorf("reconnect consumer connection, recevie done event, exit retry")
		default:
			attempt++

			val, err := cc.recreateConsumerConn(oldIdx)
			if err == nil {
				rm_log.Info(fmt.Sprintf("retry reconnect consumer connection success at attempt:%d", attempt))

				return val, nil
			}

			rm_log.Warn(fmt.Sprintf("retry reconnect consumer connection failed at attempt:%d", attempt))

			backoff := rm_utils.CalcRetryInterval(attempt, baseDelay, maxDelay)

			if rm_log.CanDebug() {
				rm_log.Debug(fmt.Sprintf("retry reconnect consumer connection attempt:%d, sleeping for:%v before next retry", attempt, backoff))
			}

			time.Sleep(backoff)
		}
	}
}

func (cc *ConsumerClient) recreateConsumerConn(oldIdx int) (*consumerConn, error) {
	floorConn, err := rm_utils.CreateFloorConn(cc.cfg)
	if err != nil {
		return nil, err
	}
	return cc.createConsumerConn(oldIdx, floorConn), nil
}

func (cc *ConsumerClient) chooseConn() *consumerConn {
	cc.connMu.RLock()
	defer cc.connMu.RUnlock()

	var idx int
	if len(cc.conns) == 1 {
		idx = 0
	} else {
		idx = int(cc.chooseIncr.Add(1) % uint64(len(cc.conns)))
	}

	conn := cc.conns[idx]

	return conn
}

func (cn *consumerConn) markDirty() {
	cn.dirty.Store(true)
}

func (cn *consumerConn) isDirty() bool {
	return cn.dirty.Load()
}

func (cn *consumerConn) close() error {
	if !cn.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(cn.connClosed)

	cn.chMu.Lock()
	defer cn.chMu.Unlock()

	for _, ch := range cn.chanChildren {
		_ = ch.close()
	}
	cn.chanChildren = nil

	if !cn.conn.IsClosed() {
		return cn.conn.Close()
	}

	return nil
}

func (cn *consumerConn) valid() bool {
	return !cn.isDirty() && cn.conn != nil && !cn.conn.IsClosed()
}

func (cn *consumerConn) createConsumerChan() (*consumerChan, error) {
	if !cn.valid() {
		return nil, ErrConnectionInvalid
	}

	ch, err := cn.conn.Channel()
	if err != nil {
		return nil, err
	}

	chId := fmt.Sprintf("%s-ch-%d", cn.id, cn.chIncr.Add(1))
	cc := &consumerChan{
		id:         chId,
		ch:         ch,
		ctime:      time.Now(),
		conn:       cn,
		willExitCh: make(chan struct{}),
	}

	cn.chMu.Lock()
	cn.chanChildren[chId] = cc
	cn.chMu.Unlock()

	return cc, nil
}

func (cc *consumerChan) cancel() error {
	if cc.ch != nil {
		if !cc.canceled.CompareAndSwap(false, true) {
			return nil
		}

		// notify rabbitmq server stop send msg for this channel
		err := cc.ch.Cancel(cc.id, false)
		if err != nil {
			return err
		}

		close(cc.willExitCh)
	}

	return nil
}

func (cc *consumerChan) close() error {
	if !cc.closed.CompareAndSwap(false, true) {
		return nil
	}

	if cc.ch != nil {
		err := cc.ch.Close()
		if err != nil {
			return err
		}
		cc.ch = nil
	}
	return nil
}

func (cc *ConsumerClient) notifyAllDeliversWillExit() {
	cc.connMu.RLock()

	for _, conn := range cc.conns {
		if conn == nil {
			continue
		}
		conn.chMu.Lock()
		for _, ch := range conn.chanChildren {
			if ch == nil {
				continue
			}
			_ = ch.cancel()
		}
		conn.chMu.Unlock()
	}

	cc.connMu.RUnlock()
}

func (cc *ConsumerClient) Shutdown(ctx context.Context) error {
	if !cc.closed.CompareAndSwap(false, true) {
		return nil
	}

	rm_log.Info("consumer receive shutdown cmd, will waiting consumers consume complete")

	// stop all deliver channels firstly to stop new msg coming in
	cc.notifyAllDeliversWillExit()

	jobDone := make(chan struct{})

	exitStart := time.Now()
	go func() {
		// waiting...
		cc.processWg.Wait()
		close(jobDone)
	}()

	waitingTimeout := rm_utils.CalcExecAvaTime(ctx, 10*time.Millisecond, waitingConsumeDoneDur)
	timer := time.NewTimer(waitingTimeout)
	select {
	case <-jobDone:
		rm_log.Info(fmt.Sprintf("all consumers completed gracefully, took:%v", time.Since(exitStart)))
	case <-timer.C:
		rm_log.Warn(fmt.Sprintf("waiting all consumers complete timeout at:%v", waitingTimeout))
	}

	close(cc.done)

	cc.connMu.Lock()
	defer cc.connMu.Unlock()

	var last error
	for _, conn := range cc.conns {
		if err := conn.close(); err != nil {
			last = err
		}
	}
	cc.conns = nil

	if last == nil {
		rm_log.Info(fmt.Sprintf("rabbitmq consumer client shutdown successfully"))
	}

	return last
}

func (cc *ConsumerClient) StartConsumer(cfgItem rm_cfg.ConsumeApplyCfgItem, handler MsgHandler) error {
	channels, err := cc.createConsumeChannels(cfgItem)
	if err != nil {
		return err
	}

	for _, ch := range channels {
		if err = cc.startConsumerInChannel(ch, cfgItem, handler); err != nil {
			// clear
			for _, chToClose := range channels {
				_ = chToClose.close()
			}
			return err
		}
	}

	return nil
}

func (cc *ConsumerClient) startConsumerInChannel(ch *consumerChan, cfgItem rm_cfg.ConsumeApplyCfgItem, handler MsgHandler) error {
	if err := ch.settingQos(cfgItem); err != nil {
		return err
	}

	delivery, err := ch.prepareDeliver(cfgItem)

	if err != nil {
		return err
	}

	cc.processWg.Add(1)

	go cc.runConsumer(ch, delivery, cfgItem, handler)

	return nil
}

func (cc *ConsumerClient) runConsumer(ch *consumerChan, delivery <-chan amqp091.Delivery, cfgItem rm_cfg.ConsumeApplyCfgItem, handler MsgHandler) {

	// protective case
	defer func() {
		if r := recover(); r != nil {
			rm_log.Error(fmt.Sprintf("consumer running panic, reason:%v", r))
		}
	}()

	defer cc.processWg.Done()

	currCh := ch
	currDelivery := delivery

	// ensure that the consumer is always running and can be retried after failure
	for {
		select {
		case <-cc.done:
			return
		case <-currCh.willExitCh:
			return
		default:
		}

		if err := cc.consumeWithRetry(currCh, currDelivery, cfgItem, handler); err != nil {
			if errors.Is(err, ErrConsumerAlreadyClosed) || errors.Is(err, ErrConnectionInvalid) {
				return
			}

			// check again
			select {
			case <-currCh.willExitCh:
				return
			case <-cc.done:
				return
			default:

			}

			rm_log.Error(fmt.Sprintf("consumer for queue:%s run failed:%v, recreating...", cfgItem.Queue, err))

			// recreate channel util success
			newCh, newDelivery, ok := cc.recreateConsumerChannel(cfgItem)

			if !ok {
				// never happened
				continue
			}

			rm_log.Info(fmt.Sprintf("recreate consumer channel successfully, conn:%s, ch:%s, queue:%s", newCh.conn.id, newCh.id, cfgItem.Queue))

			cn := currCh.conn
			// let it go, help gc
			cn.chMu.Lock()
			delete(cn.chanChildren, currCh.id)
			cn.chMu.Unlock()

			// close old, use new
			_ = currCh.close()
			currCh = newCh
			currDelivery = newDelivery
		}
	}
}

func (cc *ConsumerClient) recreateConsumerChannel(cfgItem rm_cfg.ConsumeApplyCfgItem) (*consumerChan, <-chan amqp091.Delivery, bool) {
	baseDelay := time.Duration(cc.cfg.ConsumerCfg.ConnRetryDelayMills) * time.Millisecond
	attempt := 0
	maxDelay := recreateChannelMaxDelay

	for {
		select {
		case <-cc.done:
			return nil, nil, false
		default:
		}

		conn := cc.chooseConn()

		ch, err := conn.createConsumerChan()
		if err != nil {
			attempt++

			retryInterval := rm_utils.CalcRetryInterval(attempt, baseDelay, maxDelay)
			rm_log.Error(fmt.Sprintf("retry create consumer channel failed, attempt:%d, retryInterval:%v", attempt, retryInterval))
			time.Sleep(retryInterval)

			continue
		}

		if err = ch.settingQos(cfgItem); err != nil {
			attempt++

			_ = ch.close()

			retryInterval := rm_utils.CalcRetryInterval(attempt, baseDelay, maxDelay)
			rm_log.Error(fmt.Sprintf("setting qos in retry create consumer channel failed, attempt:%d, retryInterval:%v", attempt, retryInterval))

			time.Sleep(retryInterval)

			continue
		}

		delivery, err := ch.prepareDeliver(cfgItem)

		if err != nil {
			attempt++

			_ = ch.close()

			retryInterval := rm_utils.CalcRetryInterval(attempt, baseDelay, maxDelay)
			rm_log.Error(fmt.Sprintf("prepare deliver in retry create consumer channel failed, attempt:%d, retryInterval:%v", attempt, retryInterval))

			time.Sleep(retryInterval)

			continue
		}

		return ch, delivery, true
	}
}

func (cc *consumerChan) prepareDeliver(cfgItem rm_cfg.ConsumeApplyCfgItem) (<-chan amqp091.Delivery, error) {
	return cc.ch.Consume(
		cfgItem.Queue,
		cc.id,
		cfgItem.AutoAck,
		false,
		false,
		false,
		cfgItem.ConsumeArgs,
	)
}

func (cc *consumerChan) settingQos(cfgItem rm_cfg.ConsumeApplyCfgItem) error {
	if cfgItem.PrefetchCnt <= 0 {
		cfgItem.PrefetchCnt = 1
	}

	return cc.ch.Qos(cfgItem.PrefetchCnt, 0, false)
}

type MsgHandler func(delivery amqp091.Delivery)

type OnPanic func(err error)

func HandleMsgSafety(handler MsgHandler, onPanic OnPanic) MsgHandler {
	return func(delivery amqp091.Delivery) {
		defer func() {
			if r := recover(); r != nil {
				onPanic(fmt.Errorf("consume rabbitmq msg panic:%v", r))
			}
		}()

		handler(delivery)
	}
}

func (cc *ConsumerClient) consumeWithRetry(
	ch *consumerChan,
	delivery <-chan amqp091.Delivery,
	cfgItem rm_cfg.ConsumeApplyCfgItem,
	handler MsgHandler,
) error {
	rm_log.Info(fmt.Sprintf("consumer:%s started for queue: %s", ch.id, cfgItem.Queue))

	chCloseChan := ch.ch.NotifyClose(make(chan *amqp091.Error, 1))

	for {
		select {
		case <-cc.done:
			return ErrConsumerAlreadyClosed
		case <-ch.willExitCh:
			if rm_log.CanTrace() {
				rm_log.Trace(fmt.Sprintf("consumer:%s receive will exit event for queue: %s", ch.id, cfgItem.Queue))
			}
			return nil
		case <-ch.conn.connClosed:
			return ErrConnectionInvalid
		case closeErr, ok := <-chCloseChan:
			if !ok {
				return fmt.Errorf("consumer:%s notify channel closed", ch.id)
			}
			if closeErr != nil {
				return fmt.Errorf("consumer:%s closed with error: %w", ch.id, closeErr)
			}
		case deliver, ok := <-delivery:
			if !ok {
				return fmt.Errorf("consumer:%s delivery channel closed", ch.id)
			}

			handler(deliver)
		}
	}
}

func (cc *ConsumerClient) createConsumeChannels(cfgItem rm_cfg.ConsumeApplyCfgItem) ([]*consumerChan, error) {
	if cfgItem.Concurrency == 0 {
		cfgItem.Concurrency = 1
	}

	channels := make([]*consumerChan, 0, cfgItem.Concurrency)
	var firstErr error

	for i := 0; i < cfgItem.Concurrency; i++ {
		conn := cc.chooseConn()
		ch, err := conn.createConsumerChan()
		if err != nil {
			firstErr = err
			break
		}
		channels = append(channels, ch)
	}

	if firstErr != nil {
		// clear
		for _, ch := range channels {
			_ = ch.close()
		}
		return nil, firstErr
	}

	return channels, nil
}

type TempFunc func(ch *amqp091.Channel) error

// auto close
func (cc *ConsumerClient) WithTempCh(tf TempFunc) error {
	cn := cc.chooseConn()

	tempCh, err := cn.conn.Channel()
	if err != nil {
		return err
	}

	defer tempCh.Close()

	return tf(tempCh)
}
