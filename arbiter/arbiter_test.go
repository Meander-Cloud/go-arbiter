package arbiter_test

import (
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-arbiter/arbiter"
)

func Test1(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	logPrefix := "Test1"

	a := arbiter.New(
		&arbiter.Options[string]{
			LogPrefix: logPrefix,
			LogDebug:  true,
			LogEvent:  true,
		},
	)

	a.Dispatch(
		func() {
			// invoked on arbiter goroutine
			log.Printf("%s", logPrefix)
		},
	)

	<-time.After(time.Second)
	a.Shutdown()
}

func Test2(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	logPrefix := "Test2"

	a := arbiter.New(
		&arbiter.Options[string]{
			LogPrefix: logPrefix,
			LogDebug:  false,
			LogEvent:  true,
		},
	)
	s := a.Scheduler()

	seqnum := atomic.Uint32{}

	<-time.After(time.Second * 2)
	a.Dispatch(
		func() {
			// invoked on arbiter goroutine
			log.Printf("%s<event>: %d", logPrefix, seqnum.Add(1))
		},
	)

	<-time.After(time.Second * 2)
	s.ProcessAsync(
		&scheduler.ScheduleAsyncEvent[string]{
			AsyncVariant: scheduler.TimerAsync(
				false,
				[]string{"timer"},
				time.Millisecond*256,
				func() {
					// invoked on arbiter goroutine
					log.Printf("%s<timer>: %d", logPrefix, seqnum.Add(1))
				},
				func(selectCount uint32) {
					// invoked on arbiter goroutine
					log.Printf("%s<timer>: released, selectCount=%d", logPrefix, selectCount)
				},
			),
		},
	)

	<-time.After(time.Second * 2)
	s.ProcessAsync(
		&scheduler.ScheduleAsyncEvent[string]{
			AsyncVariant: scheduler.TickerAsync(
				false,
				[]string{"ticker"},
				time.Millisecond*333,
				func() {
					// invoked on arbiter goroutine
					log.Printf("%s<ticker>: %d", logPrefix, seqnum.Add(1))
				},
				func(selectCount uint32) {
					// invoked on arbiter goroutine
					log.Printf("%s<ticker>: released, selectCount=%d", logPrefix, selectCount)
				},
			),
		},
	)

	<-time.After(time.Second * 2)
	s.ProcessAsync(
		&scheduler.ReleaseGroupEvent[string]{
			Group: "ticker",
		},
	)

	rf := func(q *scheduler.Sequence[string], stepIndex uint16, stepResult bool, sequenceResult bool) {
		if !stepResult {
			log.Printf("%s<sequence>: group=%+v, interrupted at stepIndex=%d", logPrefix, q.GroupSlice, stepIndex)
			return
		}

		if sequenceResult {
			log.Printf("%s<sequence>: group=%+v, completed", logPrefix, q.GroupSlice)
			return
		}
	}

	s.ProcessAsync(
		&scheduler.ScheduleSequenceEvent[string]{
			Sequence: scheduler.NewSequence(
				s,
				false,
				[]string{"sequence"},
				[]*scheduler.Step[string]{
					scheduler.TimerStep[string](time.Millisecond * 1500),
					scheduler.SequenceStep(
						5,
						scheduler.NewSequence(
							s,
							false,
							[]string{"sequence"},
							[]*scheduler.Step[string]{
								scheduler.ActionStep[string](func() error {
									// invoked on arbiter goroutine
									log.Printf("%s<sequence>: %d", logPrefix, seqnum.Add(1))
									return nil
								}),
								scheduler.TimerStep[string](time.Second),
							},
							rf,
							scheduler.LogProgressModeRep,
						),
					),
				},
				rf,
				scheduler.LogProgressModeRep,
			),
		},
	)

	<-time.After(time.Second * 2)
	for range 5 {
		a.Dispatch(
			func() {
				// invoked on arbiter goroutine
				log.Printf("%s<event>: %d", logPrefix, seqnum.Add(1))
			},
		)
		<-time.After(time.Second)
	}

	<-time.After(time.Second * 2)
	a.Shutdown()
}
