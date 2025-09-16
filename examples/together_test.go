package examples

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/sweemingdow/rabbitmq-go-client-pro/config"
	"github.com/sweemingdow/rabbitmq-go-client-pro/consumer"
	"github.com/sweemingdow/rabbitmq-go-client-pro/log"
	"github.com/sweemingdow/rabbitmq-go-client-pro/producer"
	"gopkg.in/yaml.v3"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var cfgExample = `
addresses: 192.168.10.100:5672 # if cluster, eg: 192.168.10.10:5672,192.168.11.100:5672,192.168.12.100:5672
virtual-host: '/'
username: {your-username}
password: {your-pwd}
log-level: debug
producer-cfg:
  conn-retry-delay-mills: 10    
  max-connections: 1             
  max-channels-per-conn: 4      
  enable-stats: true             
  enable-mandatory: true
  enable-confirm: true
consumer-cfg:
  conn-retry-delay-mills: 10
  max-connections: 1
  consume-apply-cfg-items:
    - queue: queue.test.event
      concurrency: 2
      prefetchCnt: 1
      auto-ack: false
`

var (
	pCli  *rm_producer.ProducerClient
	cCli  *rm_consumer.ConsumerClient
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

	zl := zerolog.New(os.Stdout).With().Timestamp().Str("marker", "mqLogger").Logger()
	zmq := zeroMqLogger{
		zl: zl,
	}

	// init customise log
	rm_log.InitLogger(zmq)

	// init producer client
	_pCli, err := rm_producer.NewProducerClient(cfg)
	if err != nil {
		panic(err)
	}
	pCli = _pCli

	// init consumer client with declare
	_cCli, err := rm_consumer.NewConsumerClient(
		cfg,
		func(tempCh *amqp091.Channel) (de error) {
			if de = tempCh.ExchangeDeclare(
				"direct.test.event",
				"direct",
				true,
				false,
				false,
				false,
				nil,
			); de != nil {
				return
			}

			if _, de = tempCh.QueueDeclare(
				"queue.test.event",
				true,
				false,
				false,
				false,
				nil,
			); de != nil {
				return
			}

			if de = tempCh.QueueBind(
				"queue.test.event",
				"test.event",
				"direct.test.event",
				false,
				nil,
			); de != nil {
				return
			}

			return nil
		},
	)

	if err != nil {
		panic(err)
	}

	cCli = _cCli
}

func teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// graceful shutdown, waiting all unconfirmed callback be handled
	err := pCli.Shutdown(ctx)
	if err != nil {
		panic(err)
	}

	// graceful shutdown, waiting msgHandler done
	err = cCli.Shutdown(ctx)
	if err != nil {
		panic(err)
	}
}

func init() {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
}

// for example customise mq logger with zeroLog, I love it so much
type zeroMqLogger struct {
	zl zerolog.Logger
}

// only mapping level to level
func (zmq zeroMqLogger) MqLog(ll rm_log.LogLevel, content string) {
	if ll == rm_log.TraceLevel {
		zmq.zl.Trace().Msg(content)
	} else if ll == rm_log.DebugLevel {
		zmq.zl.Debug().Msg(content)
	} else if ll == rm_log.InfoLevel {
		zmq.zl.Info().Msg(content)
	} else if ll == rm_log.WarnLevel {
		zmq.zl.Warn().Msg(content)
	} else if ll == rm_log.ErrorLevel {
		zmq.zl.Error().Msg(content)
	} else if ll == rm_log.FatalLevel {
		zmq.zl.Fatal().Msg(content)
	}
}

var (
	root = zerolog.New(os.Stdout).With().Timestamp().Logger()
	pLog = root.With().Str("marker", "producer").Logger()
	cLog = root.With().Str("marker", "consumer").Logger()
)

var confirmCallback = func(msgId string, deliverTag uint64, isAck bool) {
	pLog.Info().Msgf("receive confirm callback, msgId:%s, deliverTag:%d, isAck:%t", msgId, deliverTag, isAck)
}

var returnCallback = func(ret amqp091.Return) {
	pLog.Error().Msgf("receive return callback, msgId:%s, exchange:%s, routingKey:%s, msgBody:%s", ret.MessageId, ret.Exchange, ret.RoutingKey, string(ret.Body))
}

/*
note:
!!! To execute this test function, please ensure that the queue:queue.test.event is empty, otherwise "var cWg sync.WaitGroup" will panic
*/
func TestUseTogether(t *testing.T) {
	rm_producer.SetConfirmCallback(confirmCallback)
	rm_producer.SetReturnCallback(returnCallback)

	var (
		sendCnt = 20
		wg      sync.WaitGroup
	)

	wg.Add(2)

	// start publish
	go func() {
		defer wg.Done()

		for i := 0; i < sendCnt; i++ {
			err := rm_producer.JustConfirm(
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
						})
				},
			)

			if err != nil {
				panic(err)
			}

			time.Sleep(time.Duration(rand.Intn(191)+10) * time.Millisecond)
		}
	}()

	// start consume
	go func() {
		defer wg.Done()

		var cWg sync.WaitGroup
		cWg.Add(sendCnt)

		for _, cfgItem := range rmCfg.ConsumerCfg.ConsumeApplyCfgItems {
			err := cCli.StartConsumer(
				cfgItem,
				func(delivery amqp091.Delivery) {
					defer cWg.Done()

					cLog.Debug().Msgf("consume msg, msgId:%s, body:%s", delivery.MessageId, string(delivery.Body))

					_ = delivery.Ack(false)

					time.Sleep(time.Duration(rand.Intn(291)+30) * time.Millisecond)
				},
			)
			if err != nil {
				panic(err)
			}
		}

		cWg.Wait()
	}()

	wg.Wait()

	log.Println("task execute completed, exit now")
}
