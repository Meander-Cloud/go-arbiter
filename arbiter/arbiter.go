package arbiter

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Meander-Cloud/go-chdyn/chdyn"
	"github.com/Meander-Cloud/go-schedule/scheduler"
)

type Options[G comparable] struct {
	LogPrefix string
	LogDebug  bool
	LogEvent  bool
}

type Arbiter[G comparable] struct {
	*Options[G]
	s       *scheduler.Scheduler[G]
	eventpl sync.Pool
	eventch *chdyn.Chan[*event]
}

func New[G comparable](options *Options[G]) *Arbiter[G] {
	a := &Arbiter[G]{
		Options: options,
		s: scheduler.NewScheduler[G](
			&scheduler.Options{
				LogPrefix: options.LogPrefix,
				LogDebug:  options.LogDebug,
			},
		),
		eventpl: sync.Pool{
			New: func() any {
				return newEvent()
			},
		},
		eventch: chdyn.New(
			&chdyn.Options[*event]{
				InSize:    chdyn.InSize,
				OutSize:   chdyn.OutSize,
				LogPrefix: options.LogPrefix,
				LogDebug:  options.LogDebug,
			},
		),
	}

	// add eventch
	a.s.ProcessAsync(
		&scheduler.ScheduleAsyncEvent[G]{
			AsyncVariant: scheduler.NewAsyncVariant(
				false,
				nil,
				a.eventch.Out(),
				func(_ *scheduler.Scheduler[G], _ *scheduler.AsyncVariant[G], recv interface{}) {
					a.handle(recv)
				},
				func(_ *scheduler.Scheduler[G], v *scheduler.AsyncVariant[G]) {
					log.Printf("%s: eventch released, selectCount=%d", a.LogPrefix, v.SelectCount)
					a.eventch.Stop()
				},
			),
		},
	)

	// ownership of internal state is transferred to scheduler goroutine
	a.s.RunAsync()

	return a
}

func (a *Arbiter[G]) Shutdown() {
	a.s.Shutdown() // wait
}

func (a *Arbiter[G]) Scheduler() *scheduler.Scheduler[G] {
	return a.s
}

func (a *Arbiter[G]) getEvent() *event {
	evtAny := a.eventpl.Get()
	evt, ok := evtAny.(*event)
	if !ok {
		err := fmt.Errorf("%s: failed to cast event, evtAny=%#v", a.LogPrefix, evtAny)
		log.Printf("%s", err.Error())
		panic(err)
	}
	return evt
}

func (a *Arbiter[G]) returnEvent(evt *event) {
	// recycle event
	evt.reset()
	a.eventpl.Put(evt)
}

// scheduler goroutine
func (a *Arbiter[G]) handle(recv interface{}) {
	evt, ok := recv.(*event)
	if !ok {
		log.Printf("%s: failed to cast event, recv=%#v", a.LogPrefix, recv)
		return
	}
	defer a.returnEvent(evt)

	t1 := time.Now().UTC()

	func() {
		defer func() {
			rec := recover()
			if rec != nil {
				log.Printf(
					"%s: functor recovered from panic: %+v",
					a.LogPrefix,
					rec,
				)
			}
		}()
		evt.f()
	}()

	t2 := time.Now().UTC()

	// log event lifecycle
	if a.LogEvent {
		log.Printf(
			"%s: evtQueueWait=%dµs, evtFuncElapsed=%dµs",
			a.LogPrefix,
			t1.Sub(evt.t0).Microseconds(),
			t2.Sub(t1).Microseconds(),
		)
	}
}

// any goroutine
func (a *Arbiter[G]) Dispatch(f func()) {
	evt := a.getEvent()
	evt.f = f
	evt.t0 = time.Now().UTC()

	a.eventch.In() <- evt
}
