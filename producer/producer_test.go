package rm_producer

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
	rm_cfg "github.com/sweemingdow/rabbitmq-go-client-pro/config"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var cfgExample = `
addresses: 192.168.10.100:5672 # if cluster, eg: 192.168.10.100:5672,192.168.11.100:5672,192.168.12.100:5672
virtual-host: '/'
username: mgr
password: mgr123
log-level: trace
producer-cfg:
  conn-retry-delay-mills: 10    
  recover-mills-after-suspend: 8000
  max-connections: 1             
  max-channels-per-conn: 1      
  enable-stats: true # start statistic           
  enable-mandatory: true
  enable-confirm: true
`

var (
	pc    *ProducerClient
	rmCfg rm_cfg.RabbitmqCfg
)

func TestMain(m *testing.M) {
	setup()

	code := m.Run()

	teardown()

	os.Exit(code)
}

func setup() {
	// parse yaml
	var cfg rm_cfg.RabbitmqCfg
	err := yaml.Unmarshal([]byte(cfgExample), &cfg)
	if err != nil {
		panic(err)
	}

	rmCfg = cfg

	cli, err := NewProducerClient(cfg)
	if err != nil {
		panic(err)
	}
	pc = cli
}

func teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// graceful shutdown, waiting all unconfirmed callback be handled
	err := pc.Shutdown(ctx)
	if err != nil {
		panic(err)
	}
}

func TestProducerBaseUsed(t *testing.T) {
	// with default 4 seconds timeout
	err := pc.WithDefaultUse(
		func(ch *amqp091.Channel) error {
			// be transparent to callers
			return ch.Publish(
				"direct.test.event",
				"test.event",
				false,
				false,
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
				})
		},
	)

	if err != nil {
		panic(err)
	}

	// or customize the timeout
	err = pc.WithUse(
		1*time.Second,
		func(ch *amqp091.Channel) error {
			// be transparent to callers
			return ch.Publish(
				"direct.test.event",
				"test.event",
				false,
				false,
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"name":"swim+","desc":"swimming dog"}`),
				})
		})

	if err != nil {
		panic(err)
	}

	// with confirm
	// make sure confirm mode is configured: enable-confirm: true
	// must set confirm callback
	SetConfirmCallback(func(msgId string, deliverTag uint64, isAck bool) {
		// msgId may be helpful to you
		log.Printf("receive  confirm callback, msgId:%s, deliverTag:%d, isAck:%t\n", msgId, deliverTag, isAck)
	})

	for i := 0; i < 2; i++ {
		/*
			The caller specifies the msgId and timeout period.
			The msgId will be passed in confirmCallback.
		*/
		err = pc.WithConfirm(
			uuid.New().String(),
			8*time.Second,
			func(msgId string, ch *amqp091.Channel) error {
				return ch.Publish(
					"direct.test.event",
					"test.event",
					true,
					false,
					amqp091.Publishing{
						ContentType: "application/json",
						Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
						MessageId:   msgId,                                           // in confirm mode, use the msgId for MessageId is a better choice
					},
				)
			},
		)

		if err != nil {
			panic(err)
		}

		// with default 4 seconds timeout
		err = pc.WithConfirmDefault(
			uuid.New().String(),
			func(msgId string, ch *amqp091.Channel) error {
				return ch.Publish(
					"direct.test.event",
					"test.event",
					true,
					false,
					amqp091.Publishing{
						ContentType: "application/json",
						Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
						MessageId:   msgId,                                           // in confirm mode, use the msgId for MessageId is a better choice
					},
				)
			},
		)

		if err != nil {
			panic(err)
		}

		// don't care detail, just confirm
		err = pc.JustConfirm(
			// default generate msgId in JustConfirm
			func(msgId string, ch *amqp091.Channel) error {
				return ch.Publish(
					"direct.test.event",
					"test.event",
					true,
					false,
					amqp091.Publishing{
						ContentType: "application/json",
						Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
						MessageId:   msgId,                                           // in confirm mode, use the msgId for MessageId is a better choice
					},
				)
			},
		)

		if err != nil {
			panic(err)
		}
	}

	// even complete self-control, but this is not recommended
	pch, err := pc.GetProduceCh(2 * time.Second)
	if err != nil {
		panic(err)
	}
	//be sure to return it
	defer pc.PutProduceCh(pch)

	err = pch.ch.Publish(
		"direct.test.event",
		"test.event",
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        []byte(`{"name":"swim++","desc":"swimming dog"}`),
		})

	if err != nil {
		panic(err)
	}
}

func TestProducerAutoReconnect(t *testing.T) {
	errExit := make(chan error, 1)

	go func() {
		for {
			time.Sleep(time.Duration(rand.Intn(800-100+1)+100) * time.Millisecond)
			e := pc.WithUse(
				4*time.Second,
				func(ch *amqp091.Channel) error {
					return ch.Publish(
						"direct.test.event",
						"test.event",
						false,
						false,
						amqp091.Publishing{
							ContentType: "application/json",
							Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
						})
				},
			)

			if e != nil {
				// ignore timeout error
				if errors.Is(e, AcquireChannelTimeoutErr) {
					continue
				}

				errExit <- e
				return
			}
		}
	}()

	select {
	case err := <-errExit:
		log.Printf("exit with error:%v\n", err)
		return
	case <-time.After(180 * time.Second):
		log.Println("timeout exit now")
	}
}

func TestNewProducerReturn(t *testing.T) {
	SetConfirmCallback(func(msgId string, deliverTag uint64, isAck bool) {
		log.Printf("confirm callback, msgId:%s, deliverTag:%d, isAck:%t\n", msgId, deliverTag, isAck)
	})

	SetReturnCallback(func(ret amqp091.Return) {
		log.Printf("ret:%v had be returned\n", ret)
	})

	e := pc.JustConfirm(
		func(msgId string, ch *amqp091.Channel) error {
			return ch.Publish(
				"direct.test.event",
				"test.event_v1", // correct: test.event
				true,            // must true otherwise can't receive returnCallback
				false,
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
					MessageId:   msgId,                                           // in confirm mode, use the msgId for MessageId is a better choice
				},
			)
		},
	)

	if e != nil {
		panic(e)
	}
}

/*
make sure the following configuration is correct

	enable-mandatory: false
	enable-confirm: false
*/
func TestProducerNormalModeConcurrencyPublish(t *testing.T) {
	var (
		sendMsgCnt      = 100000
		sendConcurrency = 50
		ss              = time.Now()
	)

	var grp errgroup.Group
	grp.SetLimit(sendConcurrency)

	for i := 0; i < sendMsgCnt; i++ {
		grp.Go(func() error {
			e := pc.WithUse(
				4*time.Second,
				func(ch *amqp091.Channel) error {
					return ch.Publish(
						"direct.test.event",
						"test.event",
						false,
						false,
						amqp091.Publishing{
							ContentType: "application/json",
							Body:        []byte(`{"name":"swim","desc":"swimming dog"}`), // simple msg body
						})
				},
			)

			if e != nil {
				return e
			}

			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		fmt.Printf("publish error:%v", err)
		return
	}

	chCnt := rmCfg.ProducerCfg.MaxConnections * rmCfg.ProducerCfg.MaxChannelsPerConn

	log.Printf("send %d messages with %d connections and %d channels in %d concurrency, took:%v", sendMsgCnt, rmCfg.ProducerCfg.MaxConnections, chCnt, sendConcurrency, time.Since(ss))
	log.Println(pc.StatsInfo())

	pc.ResetStatsInfo()
}

/*
make sure the following configuration is correct

	enable-mandatory: true
	enable-confirm: true
*/
func TestProducerConfirmModeConcurrencyPublish(t *testing.T) {
	var wg sync.WaitGroup

	SetConfirmCallback(func(msgId string, deliverTag uint64, isAck bool) {
		defer wg.Done()
	})

	var (
		sendMsgCnt      = 100000
		sendConcurrency = 50
		ss              = time.Now()
	)

	var grp errgroup.Group
	grp.SetLimit(sendConcurrency)
	wg.Add(sendMsgCnt)

	for i := 0; i < sendMsgCnt; i++ {
		grp.Go(func() error {
			e := pc.WithConfirm(
				uuid.New().String(),
				8*time.Second,
				func(msgId string, ch *amqp091.Channel) error {
					return ch.Publish(
						"direct.test.event",
						"test.event",
						true,
						false,
						amqp091.Publishing{
							ContentType: "application/json",
							Body:        []byte(`{"name":"swim","desc":"swimming dog"}`),
							MessageId:   msgId,
						},
					)
				},
			)

			if e != nil {
				return e
			}

			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		fmt.Printf("publish error:%v", err)
		return
	}

	chCnt := rmCfg.ProducerCfg.MaxConnections * rmCfg.ProducerCfg.MaxChannelsPerConn

	log.Printf("send %d messages with %d connections and %d channels in %d concurrency, took:%v", sendMsgCnt, rmCfg.ProducerCfg.MaxConnections, chCnt, sendConcurrency, time.Since(ss))
	log.Println(pc.StatsInfo())

	pc.ResetStatsInfo()

	// until all confirm done
	wg.Wait()

	log.Printf("all confirm done, exit now!\n")
}

func TestOneConnOneChInvalid(t *testing.T) {
	SetConfirmCallback(func(msgId string, deliverTag uint64, isAck bool) {
		log.Printf("confirm callback, msgId:%s, deliverTag:%d, isAck:%t\n", msgId, deliverTag, isAck)
	})

	SetReturnCallback(func(ret amqp091.Return) {
		log.Printf("ret:%v had be returned\n", ret)
	})

	err := pc.WithConfirmDefault(
		uuid.New().String(),
		func(msgId string, ch *amqp091.Channel) error {
			return ch.Publish(
				"direct.test.event",
				"test.event",
				true,
				false,
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"name":"swim","desc":"swimming dog1"}`),
					MessageId:   msgId,
				},
			)
		},
	)

	if err != nil {
		log.Printf("publish msg1 err:%v\n", err)
	}

	log.Println("publish msg1 success")

	time.Sleep(2 * time.Second)

	err = pc.WithConfirmDefault(
		uuid.New().String(),
		func(msgId string, ch *amqp091.Channel) error {
			return ch.Publish(
				"direct.test.even", // correct: direct.test.event
				"test.event",
				true,
				false,
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"name":"swim","desc":"swimming dog11"}`),
					MessageId:   msgId,
				},
			)
		},
	)

	if err != nil {
		log.Printf("publish msg2 err:%v\n", err)
	}

	log.Println("publish msg2 success")

	time.Sleep(15 * time.Second)

	err = pc.WithConfirmDefault(
		uuid.New().String(),
		func(msgId string, ch *amqp091.Channel) error {
			return ch.Publish(
				"direct.test.event",
				"test.event",
				true,
				false,
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(`{"name":"swim","desc":"swimming dog111"}`),
					MessageId:   msgId,
				},
			)
		},
	)

	if err != nil {
		log.Printf("publish msg3 err:%v\n", err)
	}

	log.Println("publish msg3 success")

}
