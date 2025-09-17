package rm_producer

import (
	"fmt"
	"github.com/sweemingdow/rabbitmq-go-client-pro/utils"
	"sync"
	"sync/atomic"
	"time"
)

type statsMetrics struct {
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
}

var smp = newStatsMetrics()

func newStatsMetrics() *statsMetrics {
	sm := &statsMetrics{}
	sm.acquireChMinTook.Store(^uint32(0))
	sm.acquireChMaxTook.Store(0)
	sm.putChMinTook.Store(^uint32(0))
	sm.putChMaxTook.Store(0)
	return sm
}

func setEnableStats(enable bool) {
	smp.enableStats = enable
}

func (sm *statsMetrics) updateAcquireStatsState(took time.Duration) {
	tookUs := uint32(took.Microseconds())
	sm.acquireChTotalTimes.Add(1)
	sm.acquireChTotalTook.Add(uint64(tookUs))
	rm_utils.CASUpdateUi32Min(&sm.acquireChMinTook, tookUs)
	rm_utils.CASUpdateUi32Max(&sm.acquireChMaxTook, tookUs)
}

func (sm *statsMetrics) updatePutStatsState(took time.Duration) {
	tookUs := uint32(took.Microseconds())
	sm.putChTotalTimes.Add(1)
	sm.putChTotalTook.Add(uint64(tookUs))
	rm_utils.CASUpdateUi32Min(&sm.putChMinTook, tookUs)
	rm_utils.CASUpdateUi32Max(&sm.putChMaxTook, tookUs)
}

func (pc *ProducerClient) StatsInfo() string {
	acquireTotalTook := smp.acquireChTotalTook.Load()
	acquireTotalTimes := smp.acquireChTotalTimes.Load()
	acquireAvgTime := acquireTotalTook / acquireTotalTimes

	putTotalTook := smp.putChTotalTook.Load()
	putTotalTimes := smp.putChTotalTimes.Load()
	putAvgTime := putTotalTook / putTotalTimes

	return fmt.Sprintf("producer pool stats info, enableConfirm:%t, enableMandatory:%t, acquire[totalTook=%dus, totalTimes=%d, avg=%dus, minTook=%dus, maxTook=%dus], put:[totalTook=%dus, totalTimes=%d, avg=%dus, minTook=%dus, maxTook=%dus], recreate:[recreateCnt=%d]",
		pc.enableConfirm,
		pc.enableMandatory,
		acquireTotalTook,
		acquireTotalTimes,
		acquireAvgTime,
		smp.acquireChMinTook.Load(),
		smp.acquireChMaxTook.Load(),
		putTotalTook,
		putTotalTimes,
		putAvgTime,
		smp.putChMinTook.Load(),
		smp.putChMaxTook.Load(),
		smp.recreateChCntAfterWaiting.Load(),
	)
}

func (pc *ProducerClient) ResetStatsInfo() {
	smp.mu.Lock()
	smp.acquireChMinTook.Store(^uint32(0))
	smp.acquireChMaxTook.Store(0)
	smp.acquireChTotalTook.Store(0)
	smp.acquireChTotalTimes.Store(0)
	smp.putChMinTook.Store(^uint32(0))
	smp.putChMaxTook.Store(0)
	smp.putChTotalTook.Store(0)
	smp.putChTotalTimes.Store(0)
	smp.recreateChCntAfterWaiting.Store(0)
	smp.mu.Unlock()
}
