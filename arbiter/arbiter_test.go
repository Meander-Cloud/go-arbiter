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
				&scheduler.TimerAsyncArgs[string]{
					ReleaseGroup: false,
					GroupSlice:   []string{"timer"},
					Delay:        time.Millisecond * 256,
					SelectFunc: func() {
						// invoked on arbiter goroutine
						log.Printf("%s<timer>: %d", logPrefix, seqnum.Add(1))
					},
					ReleaseFunc: func(selectCount uint32) {
						// invoked on arbiter goroutine
						log.Printf("%s<timer>: released, selectCount=%d", logPrefix, selectCount)
					},
				},
			),
		},
	)

	<-time.After(time.Second * 2)
	s.ProcessAsync(
		&scheduler.ScheduleAsyncEvent[string]{
			AsyncVariant: scheduler.TickerAsync(
				&scheduler.TickerAsyncArgs[string]{
					ReleaseGroup: false,
					GroupSlice:   []string{"ticker"},
					Interval:     time.Millisecond * 333,
					SelectFunc: func() {
						// invoked on arbiter goroutine
						log.Printf("%s<ticker>: %d", logPrefix, seqnum.Add(1))
					},
					ReleaseFunc: func(selectCount uint32) {
						// invoked on arbiter goroutine
						log.Printf("%s<ticker>: released, selectCount=%d", logPrefix, selectCount)
					},
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

	step_rf := func(p *scheduler.Step[string], result bool) {
		log.Printf(
			"%s<sequence>: group=%s, step<%d/%d>, type=%s, rep<%d/%d>, step %s",
			logPrefix,
			p.PrintGroup(),
			p.StepNum(),
			p.StepLen(),
			p.Descriptor(),
			p.RepNum(),
			p.RepTotal(),
			func() string {
				if result {
					return "completed"
				} else {
					return "interrupted"
				}
			}(),
		)
	}

	seq_rf := func(q *scheduler.Sequence[string], result bool) {
		log.Printf(
			"%s<sequence>: group=%s, step<%d/%d>, sequence %s",
			logPrefix,
			q.PrintGroup(),
			q.StepNum(),
			q.StepLen(),
			func() string {
				if result {
					return "completed"
				} else {
					return "interrupted"
				}
			}(),
		)
	}

	s.ProcessAsync(
		&scheduler.ScheduleSequenceEvent[string]{
			Sequence: scheduler.NewSequence(
				&scheduler.SequenceArgs[string]{
					Scheduler:    s,
					InheritGroup: false,
					ReleaseGroup: false,
					GroupSlice:   []string{"sequence"},
					StepSlice: []*scheduler.Step[string]{
						scheduler.TimerStep(&scheduler.TimerStepArgs[string]{Delay: time.Millisecond * 1500}),
						scheduler.SequenceStep(
							&scheduler.SequenceStepArgs[string]{
								Sequence: scheduler.NewSequence(
									&scheduler.SequenceArgs[string]{
										Scheduler:    s,
										InheritGroup: true,
										ReleaseGroup: false,
										GroupSlice:   nil,
										StepSlice: []*scheduler.Step[string]{
											scheduler.ActionStep(&scheduler.ActionStepArgs[string]{Action: func() error {
												// invoked on arbiter goroutine
												log.Printf("%s<sequence>: %d", logPrefix, seqnum.Add(1))
												return nil
											}}),
											scheduler.TimerStep(&scheduler.TimerStepArgs[string]{Delay: time.Second}),
										},
										StepResultFunc:     step_rf,
										SequenceResultFunc: seq_rf,
										LogProgressMode:    scheduler.LogProgressModeRep,
									},
								),
								RepTotal: 5,
							},
						),
					},
					StepResultFunc:     step_rf,
					SequenceResultFunc: seq_rf,
					LogProgressMode:    scheduler.LogProgressModeRep,
				},
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
