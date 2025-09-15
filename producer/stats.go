package rm_producer

import (
	"fmt"
	"github.com/sweemingdow/rabbitmq-go-client-pro/utils"
	"sync"
	"sync/atomic"
	"time"
)

var (
	enableStats               bool
	acquireChTotalTook        atomic.Uint64
	acquireChMinTook          atomic.Uint32
	acquireChMaxTook          atomic.Uint32
	acquireChTotalTimes       atomic.Uint64
	putChTotalTook            atomic.Uint64
	putChMinTook              atomic.Uint32
	putChMaxTook              atomic.Uint32
	putChTotalTimes           atomic.Uint64
	recreateChCntAfterWaiting atomic.Uint32
	mu                        sync.Mutex
)

func init() {
	acquireChMinTook.Store(^uint32(0))
	acquireChMaxTook.Store(0)
	putChMinTook.Store(^uint32(0))
	putChMaxTook.Store(0)
}

func setEnableStats(enable bool) {
	enableStats = enable
}

func updateAcquireStatsState(took time.Duration) {
	tookUs := uint32(took.Microseconds())
	acquireChTotalTimes.Add(1)
	acquireChTotalTook.Add(uint64(tookUs))
	rm_utils.CASUpdateUi32Min(&acquireChMinTook, tookUs)
	rm_utils.CASUpdateUi32Max(&acquireChMaxTook, tookUs)
}

func updatePutStatsState(took time.Duration) {
	tookUs := uint32(took.Microseconds())
	putChTotalTimes.Add(1)
	putChTotalTook.Add(uint64(tookUs))
	rm_utils.CASUpdateUi32Min(&putChMinTook, tookUs)
	rm_utils.CASUpdateUi32Max(&putChMaxTook, tookUs)
}

func StatsInfo() string {
	acquireTotalTook := acquireChTotalTook.Load()
	acquireTotalTimes := acquireChTotalTimes.Load()
	acquireAvgTime := acquireTotalTook / acquireTotalTimes

	putTotalTook := putChTotalTook.Load()
	putTotalTimes := putChTotalTimes.Load()
	putAvgTime := putTotalTook / putTotalTimes

	return fmt.Sprintf("producer pool stats info, enableConfirm:%t, enableMandatory:%t, acquire[totalTook=%dus, totalTimes=%d, avg=%dus, minTook=%dus, maxTook=%dus], put:[totalTook=%dus, totalTimes=%d, avg=%dus, minTook=%dus, maxTook=%dus], recreate:[recreateCnt=%d]",
		pCli.enableConfirm,
		pCli.enableMandatory,
		acquireTotalTook,
		acquireTotalTimes,
		acquireAvgTime,
		acquireChMinTook.Load(),
		acquireChMaxTook.Load(),
		putTotalTook,
		putTotalTimes,
		putAvgTime,
		putChMinTook.Load(),
		putChMaxTook.Load(),
		recreateChCntAfterWaiting.Load(),
	)
}

func ResetStatsInfo() {
	mu.Lock()
	acquireChMinTook.Store(^uint32(0))
	acquireChMaxTook.Store(0)
	acquireChTotalTook.Store(0)
	acquireChTotalTimes.Store(0)
	putChMinTook.Store(^uint32(0))
	putChMaxTook.Store(0)
	putChTotalTook.Store(0)
	putChTotalTimes.Store(0)
	recreateChCntAfterWaiting.Store(0)
	mu.Unlock()
}
