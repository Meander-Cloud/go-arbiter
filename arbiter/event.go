package arbiter

import "time"

type event struct {
	f  func()
	t0 time.Time
}

func newEvent() *event {
	return &event{
		f:  nil,
		t0: time.Time{},
	}
}

// scheduler goroutine
func (e *event) reset() {
	e.f = nil
	e.t0 = time.Time{}
}
