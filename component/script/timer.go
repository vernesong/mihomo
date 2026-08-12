package script

import (
	"math"
	"time"

	"github.com/grafana/sobek"
)

const maxTimerDelayMilliseconds = int64(math.MaxInt64) / int64(time.Millisecond)

func (h *runtimeHost) installTimerGlobals() error {
	if err := h.vm.Set("setTimeout", h.setTimeout); err != nil {
		return err
	}
	return h.vm.Set("clearTimeout", h.clearTimeout)
}

func (h *runtimeHost) setTimeout(call sobek.FunctionCall) sobek.Value {
	if len(call.Arguments) == 0 {
		panic(h.vm.NewTypeError("setTimeout requires a callback"))
	}
	callback, ok := sobek.AssertFunction(call.Argument(0))
	if !ok {
		panic(h.vm.NewTypeError("setTimeout callback must be a function"))
	}
	delayMilliseconds := int64(0)
	if len(call.Arguments) > 1 {
		delayMilliseconds = call.Argument(1).ToInteger()
		if delayMilliseconds < 0 {
			delayMilliseconds = 0
		} else if delayMilliseconds > maxTimerDelayMilliseconds {
			delayMilliseconds = maxTimerDelayMilliseconds
		}
	}
	arguments := append([]sobek.Value(nil), call.Arguments[2:]...)
	h.nextTimer++
	timerID := h.nextTimer
	h.timers[timerID] = time.AfterFunc(time.Duration(delayMilliseconds)*time.Millisecond, func() {
		job := func(host *runtimeHost) error {
			if _, found := host.timers[timerID]; !found {
				return nil
			}
			delete(host.timers, timerID)
			_, err := callback(sobek.Undefined(), arguments...)
			return err
		}
		select {
		case h.jobs <- job:
		case <-h.ctx.Done():
		}
	})
	return h.vm.ToValue(timerID)
}

func (h *runtimeHost) clearTimeout(call sobek.FunctionCall) sobek.Value {
	if len(call.Arguments) == 0 {
		return sobek.Undefined()
	}
	timerID := call.Argument(0).ToInteger()
	if timer, found := h.timers[timerID]; found {
		timer.Stop()
		delete(h.timers, timerID)
	}
	return sobek.Undefined()
}

func (h *runtimeHost) stopTimers() {
	for timerID, timer := range h.timers {
		timer.Stop()
		delete(h.timers, timerID)
	}
}
