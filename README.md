The current Go RabbitMQ client ecosystem? Let’s be honest — it’s underwhelming.
Most libraries cling to single connections and single channels, with zero regard for production-grade connection management.
Automatic reconnection? Reliable publisher confirms? Forget it.
Fed up with bandaids, I built go-rabbitmq-client-pro — a framework that actually survives real-world chaos.

#### <font style="color:rgb(15, 17, 21);">Feature List</font>

1. **<font style="color:rgb(15, 17, 21);">High Performance & Thread-Safe</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Multi-connection and multi-channel architecture designed for high throughput with goroutine safety.</font>
2. **<font style="color:rgb(15, 17, 21);">Producer-Consumer Decoupling</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Producers and consumers operate independently and can function in isolation or together.</font>
3. **<font style="color:rgb(15, 17, 21);">Automatic Reconnection</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Built-in automatic reconnection for both producers and consumers.</font>
4. **<font style="color:rgb(15, 17, 21);">Concurrent Consumption</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Consumers support concurrent processing to significantly increase message throughput.</font>
5. **<font style="color:rgb(15, 17, 21);">Simple & Transparent API</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Easy-to-use API that remains transparent to the caller.</font>
6. **<font style="color:rgb(15, 17, 21);">Comprehensive Callbacks</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Producers provide full callback support, including Confirm and Return listeners.</font>
7. **<font style="color:rgb(15, 17, 21);">Basic Statistics</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Includes essential message metrics and statistics.</font>
8. **<font style="color:rgb(15, 17, 21);">Graceful Shutdown</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Clean and graceful shutdown support for both producers and consumers.</font>
9. **<font style="color:rgb(15, 17, 21);">Minimal Dependency (Haxmap)</font>**<font style="color:rgb(15, 17, 21);">  
   </font><font style="color:rgb(15, 17, 21);">Only requires the high-performance, concurrent map library </font>[<font style="color:rgb(15, 17, 21);">haxmap</font>](https://github.com/alphadose/haxmap)
10. **<font style="color:rgb(15, 17, 21);">Facade Logging</font>**<font style="color:rgb(15, 17, 21);">  
    </font><font style="color:rgb(15, 17, 21);">Logging facade allows for easy integration with any logging system; uses the standard log package by default.</font>

#### Producer Client Performance Benchmark Summary

| Messages | Concurrency | Conns × Chans | Unconfirm Mode Time | Confirm  Mode Time | Performance Drop | Unconfirm Throughput | Confirm Throughput |
| -------- | ----------- | ------------- | ------------------- | ------------------ | ---------------- | -------------------- | ------------------ |
| 100K     | 50          | 2 × 80 (160)  | 0.68s               | 0.81s              | -19%             | 147,059 msg/s        | 123,457 msg/s      |
| 1M       | 50          | 2 × 80 (160)  | 20.99s              | 37.10s             | -77%             | 47,642 msg/s         | 26,954 msg/s       |
| 2M       | 100         | 2 × 80 (160)  | 45.30s              | 77.14s             | -70%             | 44,150 msg/s         | 25,927 msg/s       |
| 4M       | 200         | 2 × 80 (160)  | 90.48s              | 155.99s            | -72%             | 44,208 msg/s         | 25,641 msg/s       |
| 8M       | 400         | 2 × 80 (160)  | 182.71s             | 318.94s            | -75%             | 43,784 msg/s         | 25,083 msg/s       |


+ Key Findings
  - Unconfirm Mode Performance: 43,784 - 47,642 msg/s
  - Confirm Mode Performance: 25,083 - 26,954 msg/s
  - Throughput Impact: ~43% reduction with Confirm mode
  - Stability: Zero connection recreations under all load conditions.

8M Unconfirmed Mode
![8M-Unconfirm-Mode](https://github.com/sweemingdow/rabbitmq-go-client-pro/blob/main/assets/images/8M-unconfirmed.png)

8M Confirmed Mode
![8M-Confirm-Mode](https://github.com/sweemingdow/rabbitmq-go-client-pro/blob/main/assets/images/8M-confirmed.png)

#### Quick Start

<font style="color:rgb(31, 35, 40);">You need Golang 1.20.x or above</font>

##### Producer

```go
var cfgExample = `
addresses: 127.0.0.1:5672 # if cluster, eg: 192.168.10.100:5672,192.168.11.100:5672,192.168.12.100:5672
virtual-host: '/'
username: {your-username}
password: {your-pwd}
log-level: trace
producer-cfg:
  conn-retry-delay-mills: 10    
  max-connections: 2             
  max-channels-per-conn: 80      
  enable-stats: true # start statistic, recommended to enable it           
  enable-mandatory: true
  enable-confirm: true
`

// parse yaml
var cfg rm_cfg.RabbitmqCfg
err := yaml.Unmarshal([]byte(cfgExample), &cfg)
if err != nil {
    panic(err)
}

// init producer client with config
cli, err := NewProducerClient(cfg)

// with default 4 seconds timeout
err := WithDefaultUse(
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
err = WithUse(
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
    err = WithConfirm(
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
                    MessageId:   msgId,                                          
                },
            )
        },
    )

if err != nil {
    panic(err)
}

// with default 4 seconds timeout
err = WithConfirmDefault(
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
                MessageId:   msgId,                                           
            },
        )
    },
)

if err != nil {
    panic(err)
}

// don't care detail, just confirm
err = JustConfirm(
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
                MessageId:   msgId,                                           
            },
        )
    },
)

if err != nil {
    panic(err)
}
```

##### Consumer

```go
var cfgExample = `
addresses: 127.0.0.1:5672 # if cluster, eg: 192.168.10.10:5672,192.168.11.100:5672,192.168.12.100:5672
virtual-host: '/'
username: {your-username}
password: {your-pwd}
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
var cfg rm_cfg.RabbitmqCfg
err := yaml.Unmarshal([]byte(cfgExample), &cfg)
if err != nil {
    panic(err)
}

// declare hook func
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
    
	return
}

// init consumer client with config and declare hook
cli, err := NewConsumerClient(cfg, declareFunc)

// it's not real received msg count
// may have more messages received after main goroutine exit, until graceful shutdown
var receivedCnt atomic.Uint32

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
```

##### others

For more detailed usage, see test cases.

#### Custom Logging

```go
// for example customise mq logger with zeroLog, I love it so much
func init() {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
}

3.203
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

zl := zerolog.New(os.Stdout).With().Timestamp().Str("marker", "mqLogger").Logger()
zmq := zeroMqLogger{
    zl: zl,
}
rm_log.InitLogger(zmq)

// may output
{"level":"debug","marker":"mqLogger","time":"2025-09-15 17:59:13.176","message":"rabbitmq producer client init successfully"}
{"level":"debug","marker":"mqLogger","time":"2025-09-15 17:59:13.190","message":"rabbitmq consumer client init successfully"}
// ...
{"level":"info","marker":"mqLogger","time":"2025-09-15 17:59:15.200","message":"producer receive shutdown cmd, will waiting unconfirmed callbacks complete"}
{"level":"info","marker":"mqLogger","time":"2025-09-15 17:59:15.203","message":"rabbitmq producer client shutdown successfully"}
{"level":"info","marker":"mqLogger","time":"2025-09-15 17:59:15.203","message":"consumer receive shutdown cmd, will waiting consumers consume complete"}
{"level":"info","marker":"mqLogger","time":"2025-09-15 17:59:15.262","message":"all consumers completed gracefully, took:58.2729ms"}
{"level":"info","marker":"mqLogger","time":"2025-09-15 17:59:15.265","message":"rabbitmq consumer client shutdown successfully"}
```

#### Test log

##### Auto Reconnect

Regardless of whether it is a producer or a consumer, the management background can automatically connect and work normally when the connection is forcibly closed and rabbitmq-server is stopped and then restarted.

###### Consumer Auto Reconnect Log  

```plain
=== RUN   TestConsumerAutoReconnect
2025/09/13 12:01:22 [INFO] consumer:c-conn-1-ch-1 started for queue: queue.test.event
2025/09/13 12:01:22 [INFO] consumer:c-conn-2-ch-1 started for queue: queue.test.event
2025/09/13 12:02:04 receive msg body:{"name":"jack yellow"}
2025/09/13 12:02:13 [ERROR] receive consumer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 12:02:13 [ERROR] consumer for queue:queue.test.event run failed:consumer:c-conn-1-ch-1 closed with error: Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin", recreating...
2025/09/13 12:02:13 [ERROR] retry create consumer channel failed, attempt:1, retryInterval:11.622563ms
2025/09/13 12:02:13 [INFO] retry reconnect consumer connection success at attempt:1
2025/09/13 12:02:13 [INFO] recreate consumer connection success after receive close event, newConn:c-conn-1
2025/09/13 12:02:13 [INFO] recreate consumer channel successfully, conn:c-conn-2, ch:c-conn-2-ch-2, queue:queue.test.event
2025/09/13 12:02:13 [INFO] consumer:c-conn-2-ch-2 started for queue: queue.test.event
2025/09/13 12:02:25 receive msg body:{"name":"jack yellow"}
2025/09/13 12:02:31 receive msg body:{"name":"jack yellow1"}
2025/09/13 12:02:35 receive msg body:{"name":"jack yellow2"}
2025/09/13 12:02:40 receive msg body:{"name":"jack yellow3"}
2025/09/13 12:02:48 [ERROR] receive consumer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 12:02:48 [INFO] retry reconnect consumer connection success at attempt:1
2025/09/13 12:02:48 [INFO] recreate consumer connection success after receive close event, newConn:c-conn-3
2025/09/13 12:03:01 receive msg body:{"name":"jack yellow111"}
2025/09/13 12:03:13 [ERROR] receive consumer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 12:03:13 [ERROR] receive consumer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 12:03:13 [ERROR] consumer for queue:queue.test.event run failed:consumer:c-conn-2-ch-1 closed with error: Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'", recreating...
2025/09/13 12:03:13 [ERROR] retry create consumer channel failed, attempt:1, retryInterval:17.405273ms
2025/09/13 12:03:13 [ERROR] consumer for queue:queue.test.event run failed:consumer:c-conn-2-ch-2 closed with error: Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'", recreating...
2025/09/13 12:03:13 [ERROR] retry create consumer channel failed, attempt:1, retryInterval:10.006968ms
2025/09/13 12:03:13 [ERROR] retry create consumer channel failed, attempt:2, retryInterval:31.703139ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:2, retryInterval:36.400527ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:3, retryInterval:51.995461ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:3, retryInterval:64.374134ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:4, retryInterval:86.237321ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:4, retryInterval:109.049506ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:5, retryInterval:265.157489ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:5, retryInterval:233.754371ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:6, retryInterval:638.87178ms
2025/09/13 12:03:14 [ERROR] retry create consumer channel failed, attempt:6, retryInterval:572.645534ms
2025/09/13 12:03:15 [ERROR] retry create consumer channel failed, attempt:7, retryInterval:961.861316ms
2025/09/13 12:03:15 [ERROR] retry create consumer channel failed, attempt:7, retryInterval:1.068685476s
2025/09/13 12:03:15 [ERROR] retry create consumer channel failed, attempt:8, retryInterval:1.657682013s
2025/09/13 12:03:16 [WARN] retry reconnect consumer connection failed at attempt:1
2025/09/13 12:03:16 [DEBUG] retry reconnect consumer connection attempt:1, sleeping for:11.104249ms before next retry
2025/09/13 12:03:16 [WARN] retry reconnect consumer connection failed at attempt:1
2025/09/13 12:03:16 [DEBUG] retry reconnect consumer connection attempt:1, sleeping for:11.598918ms before next retry
2025/09/13 12:03:16 [ERROR] retry create consumer channel failed, attempt:8, retryInterval:1.521794431s
2025/09/13 12:03:17 [ERROR] retry create consumer channel failed, attempt:9, retryInterval:4.848444893s
2025/09/13 12:03:17 [ERROR] retry create consumer channel failed, attempt:9, retryInterval:2.955538006s
2025/09/13 12:03:18 [WARN] retry reconnect consumer connection failed at attempt:2
2025/09/13 12:03:18 [DEBUG] retry reconnect consumer connection attempt:2, sleeping for:31.796628ms before next retry
2025/09/13 12:03:18 [WARN] retry reconnect consumer connection failed at attempt:2
2025/09/13 12:03:18 [DEBUG] retry reconnect consumer connection attempt:2, sleeping for:35.014232ms before next retry
2025/09/13 12:03:20 [WARN] retry reconnect consumer connection failed at attempt:3
2025/09/13 12:03:20 [DEBUG] retry reconnect consumer connection attempt:3, sleeping for:71.267627ms before next retry
2025/09/13 12:03:20 [WARN] retry reconnect consumer connection failed at attempt:3
2025/09/13 12:03:20 [DEBUG] retry reconnect consumer connection attempt:3, sleeping for:46.470166ms before next retry
2025/09/13 12:03:20 [ERROR] retry create consumer channel failed, attempt:10, retryInterval:7.215188839s
2025/09/13 12:03:22 [WARN] retry reconnect consumer connection failed at attempt:4
2025/09/13 12:03:22 [DEBUG] retry reconnect consumer connection attempt:4, sleeping for:133.37688ms before next retry
2025/09/13 12:03:22 [WARN] retry reconnect consumer connection failed at attempt:4
2025/09/13 12:03:22 [DEBUG] retry reconnect consumer connection attempt:4, sleeping for:115.743759ms before next retry
2025/09/13 12:03:22 [ERROR] retry create consumer channel failed, attempt:10, retryInterval:8.794203474s
2025/09/13 12:03:24 [WARN] retry reconnect consumer connection failed at attempt:5
2025/09/13 12:03:24 [DEBUG] retry reconnect consumer connection attempt:5, sleeping for:308.259033ms before next retry
2025/09/13 12:03:24 [WARN] retry reconnect consumer connection failed at attempt:5
2025/09/13 12:03:24 [DEBUG] retry reconnect consumer connection attempt:5, sleeping for:244.249401ms before next retry
2025/09/13 12:03:26 [WARN] retry reconnect consumer connection failed at attempt:6
2025/09/13 12:03:26 [DEBUG] retry reconnect consumer connection attempt:6, sleeping for:372.286581ms before next retry
2025/09/13 12:03:26 [WARN] retry reconnect consumer connection failed at attempt:6
2025/09/13 12:03:26 [DEBUG] retry reconnect consumer connection attempt:6, sleeping for:390.53523ms before next retry
2025/09/13 12:03:27 [ERROR] retry create consumer channel failed, attempt:11, retryInterval:15.102233538s
2025/09/13 12:03:28 [INFO] retry reconnect consumer connection success at attempt:7
2025/09/13 12:03:28 [INFO] recreate consumer connection success after receive close event, newConn:c-conn-4
2025/09/13 12:03:28 [INFO] retry reconnect consumer connection success at attempt:7
2025/09/13 12:03:28 [INFO] recreate consumer connection success after receive close event, newConn:c-conn-2
2025/09/13 12:03:31 [INFO] recreate consumer channel successfully, conn:c-conn-6, ch:c-conn-6-ch-1, queue:queue.test.event
2025/09/13 12:03:31 [INFO] consumer:c-conn-6-ch-1 started for queue: queue.test.event
2025/09/13 12:03:34 receive msg body:{"name":"jack yellow1112222"}
2025/09/13 12:03:37 receive msg body:{"name":"jack yellow1112222111"}
2025/09/13 12:03:42 [INFO] recreate consumer channel successfully, conn:c-conn-5, ch:c-conn-5-ch-1, queue:queue.test.event
2025/09/13 12:03:42 [INFO] consumer:c-conn-5-ch-1 started for queue: queue.test.event
2025/09/13 12:04:22 timeout exit
--- PASS: TestConsumerAutoReconnect (180.00s)
PASS
2025/09/13 12:04:22 [TRACE] consumer:c-conn-5-ch-1 receive will exit event for queue: queue.test.event
2025/09/13 12:04:22 [INFO] will shutdown rabbitmq consumer now, waiting all consumers complete
2025/09/13 12:04:22 [TRACE] consumer:c-conn-6-ch-1 receive will exit event for queue: queue.test.event
2025/09/13 12:04:22 [INFO] all consumers completed gracefully, took:0s
```

###### Producer Auto Reconnect Log

```plain
=== RUN   TestProducerAutoReconnect
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-10 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-7 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-8 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-9 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-3 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-4 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-1 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-2 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-5 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [ERROR] receive producer channel:p-conn-1-6 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:39 [INFO] retry reconnect producer connection success at attempt:1
2025/09/13 14:06:39 [INFO] recreate producer connection success after receive close event, newConn:p-conn-1
2025/09/13 14:06:50 [ERROR] receive producer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-10 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-7 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-9 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-2 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-5 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-3 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-8 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-1 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-4 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [ERROR] receive producer channel:p-conn-2-6 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - Closed via management plugin"
2025/09/13 14:06:50 [INFO] retry reconnect producer connection success at attempt:1
2025/09/13 14:06:50 [INFO] recreate producer connection success after receive close event, newConn:p-conn-2
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-9 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-6 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-5 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-1 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-7 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-10 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-8 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-3 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-2 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-4-4 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-1 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-10 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-7 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer connection closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-8 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-3 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-2 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-4 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-6 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-9 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [ERROR] receive producer channel:p-conn-3-5 closed event, reason:Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
2025/09/13 14:06:58 [DEBUG] choose connection, conn:p-conn-3 was invalid, choose connection again+1
2025/09/13 14:06:58 [DEBUG] choose connection, conn:p-conn-4was invalid, choose connection again+2 after:8ms
2025/09/13 14:06:58 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+3 after:11.7033ms
2025/09/13 14:06:58 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+4 after:32.9831ms
2025/09/13 14:06:58 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+5 after:71.4391ms
2025/09/13 14:06:58 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+6 after:189.7312ms
2025/09/13 14:06:58 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+7 after:413.6537ms
2025/09/13 14:06:59 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+8 after:753.1849ms
2025/09/13 14:07:00 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+9 after:1.6702672s
2025/09/13 14:07:00 [WARN] retry reconnect producer connection failed at attempt:1
2025/09/13 14:07:00 [DEBUG] retry reconnect producer connection attempt:1, sleeping for:16.378052ms before next retry
2025/09/13 14:07:00 [WARN] retry reconnect producer connection failed at attempt:1
2025/09/13 14:07:00 [DEBUG] retry reconnect producer connection attempt:1, sleeping for:16.543331ms before next retry
2025/09/13 14:07:01 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+10 after:2.731892s
2025/09/13 14:07:02 [WARN] retry reconnect producer connection failed at attempt:2
2025/09/13 14:07:02 [DEBUG] retry reconnect producer connection attempt:2, sleeping for:25.780733ms before next retry
2025/09/13 14:07:02 [WARN] retry reconnect producer connection failed at attempt:2
2025/09/13 14:07:02 [DEBUG] retry reconnect producer connection attempt:2, sleeping for:24.433434ms before next retry
2025/09/13 14:07:04 [WARN] retry reconnect producer connection failed at attempt:3
2025/09/13 14:07:04 [DEBUG] retry reconnect producer connection attempt:3, sleeping for:43.88951ms before next retry
2025/09/13 14:07:04 [WARN] retry reconnect producer connection failed at attempt:3
2025/09/13 14:07:04 [DEBUG] retry reconnect producer connection attempt:3, sleeping for:76.281927ms before next retry
2025/09/13 14:07:05 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+11 after:6.7245084s
2025/09/13 14:07:05 [DEBUG] choose connection, conn:p-conn-3 was invalid, choose connection again+1
2025/09/13 14:07:05 [DEBUG] choose connection, conn:p-conn-4was invalid, choose connection again+2 after:8ms
2025/09/13 14:07:05 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+3 after:11.9886ms
2025/09/13 14:07:05 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+4 after:37.0358ms
2025/09/13 14:07:05 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+5 after:73.9489ms
2025/09/13 14:07:05 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+6 after:196.5941ms
2025/09/13 14:07:06 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+7 after:427.8642ms
2025/09/13 14:07:06 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+8 after:849.6226ms
2025/09/13 14:07:06 [WARN] retry reconnect producer connection failed at attempt:4
2025/09/13 14:07:06 [DEBUG] retry reconnect producer connection attempt:4, sleeping for:136.890628ms before next retry
2025/09/13 14:07:06 [WARN] retry reconnect producer connection failed at attempt:4
2025/09/13 14:07:06 [DEBUG] retry reconnect producer connection attempt:4, sleeping for:84.206755ms before next retry
2025/09/13 14:07:07 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+9 after:1.6326698s
2025/09/13 14:07:08 [WARN] retry reconnect producer connection failed at attempt:5
2025/09/13 14:07:08 [DEBUG] retry reconnect producer connection attempt:5, sleeping for:294.340465ms before next retry
2025/09/13 14:07:08 [WARN] retry reconnect producer connection failed at attempt:5
2025/09/13 14:07:08 [DEBUG] retry reconnect producer connection attempt:5, sleeping for:174.136484ms before next retry
2025/09/13 14:07:08 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+10 after:3.1802211s
2025/09/13 14:07:10 [WARN] retry reconnect producer connection failed at attempt:6
2025/09/13 14:07:10 [DEBUG] retry reconnect producer connection attempt:6, sleeping for:505.957958ms before next retry
2025/09/13 14:07:11 [WARN] retry reconnect producer connection failed at attempt:6
2025/09/13 14:07:11 [DEBUG] retry reconnect producer connection attempt:6, sleeping for:615.411327ms before next retry
2025/09/13 14:07:12 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+11 after:7.1726469s
2025/09/13 14:07:13 [DEBUG] choose connection, conn:p-conn-3 was invalid, choose connection again+1
2025/09/13 14:07:13 [DEBUG] choose connection, conn:p-conn-4was invalid, choose connection again+2 after:8ms
2025/09/13 14:07:13 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+3 after:15.9189ms
2025/09/13 14:07:13 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+4 after:45.5412ms
2025/09/13 14:07:13 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+5 after:91.2883ms
2025/09/13 14:07:13 [WARN] retry reconnect producer connection failed at attempt:7
2025/09/13 14:07:13 [DEBUG] retry reconnect producer connection attempt:7, sleeping for:1.155118241s before next retry
2025/09/13 14:07:13 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+6 after:163.733ms
2025/09/13 14:07:13 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+7 after:336.6352ms
2025/09/13 14:07:13 [WARN] retry reconnect producer connection failed at attempt:7
2025/09/13 14:07:13 [DEBUG] retry reconnect producer connection attempt:7, sleeping for:1.036739744s before next retry
2025/09/13 14:07:13 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+8 after:660.2118ms
2025/09/13 14:07:14 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+9 after:1.4034452s
2025/09/13 14:07:15 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+10 after:2.6240264s
2025/09/13 14:07:16 [WARN] retry reconnect producer connection failed at attempt:8
2025/09/13 14:07:16 [DEBUG] retry reconnect producer connection attempt:8, sleeping for:1.318123212s before next retry
2025/09/13 14:07:16 [WARN] retry reconnect producer connection failed at attempt:8
2025/09/13 14:07:16 [DEBUG] retry reconnect producer connection attempt:8, sleeping for:2.178473504s before next retry
2025/09/13 14:07:19 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+11 after:6.6162451s
2025/09/13 14:07:19 [WARN] retry reconnect producer connection failed at attempt:9
2025/09/13 14:07:19 [DEBUG] retry reconnect producer connection attempt:9, sleeping for:5.03508554s before next retry
2025/09/13 14:07:20 [DEBUG] choose connection, conn:p-conn-3 was invalid, choose connection again+1
2025/09/13 14:07:20 [DEBUG] choose connection, conn:p-conn-4was invalid, choose connection again+2 after:8ms
2025/09/13 14:07:20 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+3 after:15.1186ms
2025/09/13 14:07:20 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+4 after:44.0079ms
2025/09/13 14:07:20 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+5 after:81.7483ms
2025/09/13 14:07:20 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+6 after:200.2797ms
2025/09/13 14:07:20 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+7 after:329.6026ms
2025/09/13 14:07:20 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+8 after:687.8241ms
2025/09/13 14:07:20 [WARN] retry reconnect producer connection failed at attempt:9
2025/09/13 14:07:20 [DEBUG] retry reconnect producer connection attempt:9, sleeping for:3.907642842s before next retry
2025/09/13 14:07:21 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+9 after:1.6464592s
2025/09/13 14:07:23 [TRACE] choose connection, conn:p-conn-4 was invalid, choose connection again+10 after:3.6153317s
2025/09/13 14:07:24 [INFO] retry reconnect producer connection success at attempt:10
2025/09/13 14:07:24 [INFO] recreate producer connection success after receive close event, newConn:p-conn-3
2025/09/13 14:07:25 [INFO] retry reconnect producer connection success at attempt:10
2025/09/13 14:07:25 [INFO] recreate producer connection success after receive close event, newConn:p-conn-4
2025/09/13 14:07:27 [TRACE] choose connection, conn:p-conn-3 was invalid, choose connection again+11 after:7.6075938s
2025/09/13 14:09:24 timeout exit now
--- PASS: TestProducerAutoReconnect (180.00s)
PASS
2025/09/13 14:09:24 [INFO] rabbitmq producer client shutdown successfully
```

