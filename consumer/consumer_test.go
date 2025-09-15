package rm_consumer

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sweemingdow/rabbitmq-go-client-pro/config"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var cfgExample = `
addresses: 192.168.10.100:5672 # if cluster, eg: 192.168.10.10:5672,192.168.11.100:5672,192.168.12.100:5672
virtual-host: "/"
username: mgr
password: mgr123
log-level: trace
consumer-cfg:
  conn-retry-delay-mills: 10
  max-connections: 2
  consume-apply-cfg-items:
    - queue: queue.test.event
      concurrency: 2
      prefetchCnt: 1
      auto-ack: false
`

var (
	cc    *ConsumerClient
	rmCfg rm_cfg.RabbitmqCfg
)

func TestMain(m *testing.M) {
	setup()

	code := m.Run()

	teardown()

	os.Exit(code)
}

func TestConsumerBaseUse(t *testing.T) {
	// it's not real received msg count
	// may have more messages received after main goroutine exit, until graceful shutdown
	var receivedCnt atomic.Uint32

	err := cc.StartConsumer(rmCfg.ConsumerCfg.ConsumeApplyCfgItems[0], func(delivery amqp091.Delivery) {
		defer delivery.Ack(false)

		log.Printf("consume msg, msgId:%s, body:%s\n", delivery.MessageId, string(delivery.Body))
		receivedCnt.Add(1)
	})

	if err != nil {
		panic(err)
	}

	// exit after 2 seconds
	select {
	case <-time.After(2 * time.Second):
		log.Printf("timeout exit, received msg count:%d\n", receivedCnt.Load())
	}
}

func TestConsumerAutoReconnect(t *testing.T) {
	for _, appItem := range rmCfg.ConsumerCfg.ConsumeApplyCfgItems {
		err := cc.StartConsumer(appItem, func(delivery amqp091.Delivery) {
			log.Printf("receive msg body:%s\n", string(delivery.Body))
			_ = delivery.Ack(false)
		})

		if err != nil {
			panic(err)
		}
	}

	// will exit after 3 minutes
	select {
	case <-time.After(3 * time.Minute):
		log.Println("timeout exit")
	}
}

var declareFunc = func(tempCh *amqp091.Channel) (err error) {
	if err = tempCh.ExchangeDeclare(
		"direct.test.event",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return
	}

	if _, err = tempCh.QueueDeclare(
		"queue.test.event",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return
	}

	if err = tempCh.QueueBind(
		"queue.test.event",
		"test.event",
		"direct.test.event",
		false,
		nil,
	); err != nil {
		return
	}
	return nil
}

func setup() {
	var cfg rm_cfg.RabbitmqCfg
	err := yaml.Unmarshal([]byte(cfgExample), &cfg)
	if err != nil {
		panic(err)
	}

	rmCfg = cfg

	cli, err := NewConsumerClient(cfg, declareFunc)
	if err != nil {
		panic(err)
	}
	cc = cli
}

func teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	// graceful shutdown, waiting msgHandler done
	err := cc.Shutdown(ctx)
	if err != nil {
		panic(err)
	}
}
