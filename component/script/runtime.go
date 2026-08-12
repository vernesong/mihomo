package script

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/log"

	"github.com/grafana/sobek"
	"github.com/metacubex/http"
	"github.com/metacubex/http/cookiejar"
)

const (
	persistentStoreKeyLimit  = 64
	persistentStoreDataLimit = 1024 * 1024
)

type evaluationInput struct {
	request     *http.Request
	response    *http.Response
	requestID   string
	body        []byte
	bodyPresent bool
}

type runtimeJob func(*runtimeHost) error

type runtimeHost struct {
	ctx       context.Context
	vm        *sobek.Runtime
	entry     *entry
	jobs      chan runtimeJob
	jar       http.CookieJar
	startedAt time.Time
	timers    map[int64]*time.Timer
	nextTimer int64

	doneCalled   bool
	doneProvided bool
	doneValue    sobek.Value
}

func (m *Manager) evaluate(scriptEntry *entry, input evaluationInput) (patch map[string]any, resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			patch = nil
			if recoveredErr, ok := recovered.(error); ok {
				resultErr = fmt.Errorf("runtime panic: %w", recoveredErr)
			} else {
				resultErr = fmt.Errorf("runtime panic: %v", recovered)
			}
		}
	}()

	program := scriptEntry.currentProgram()
	if program == nil {
		return nil, errors.New("javascript is not loaded")
	}
	ctx, cancel := context.WithTimeout(m.ctx, scriptEntry.timeout)
	if input.request != nil {
		stopRequestCancel := context.AfterFunc(input.request.Context(), cancel)
		defer stopRequestCancel()
	}
	defer cancel()

	vm := sobek.New()
	jar, _ := cookiejar.New(nil)
	startedAt := time.Now()
	host := &runtimeHost{
		ctx:       ctx,
		vm:        vm,
		entry:     scriptEntry,
		jobs:      make(chan runtimeJob, 64),
		jar:       jar,
		startedAt: startedAt,
		timers:    make(map[int64]*time.Timer),
	}
	defer host.stopTimers()
	if err := host.installGlobals(input); err != nil {
		return nil, err
	}

	if scriptEntry.debug {
		log.Infoln("[Script] %s (%s) started", scriptEntry.name, scriptEntry.scriptType)
		defer func() {
			if resultErr == nil {
				log.Infoln("[Script] %s completed in %s", scriptEntry.name, time.Since(startedAt))
			}
		}()
	}

	interruptDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			vm.Interrupt(ctx.Err())
		case <-interruptDone:
		}
	}()
	defer close(interruptDone)

	if _, err := vm.RunProgram(program); err != nil {
		return nil, evaluationError(ctx, scriptEntry.timeout, err)
	}
	for !host.doneCalled {
		select {
		case job := <-host.jobs:
			if err := job(host); err != nil {
				return nil, evaluationError(ctx, scriptEntry.timeout, err)
			}
		case <-ctx.Done():
			return nil, evaluationError(ctx, scriptEntry.timeout, ctx.Err())
		}
	}
	if !host.doneProvided || sobek.IsUndefined(host.doneValue) || sobek.IsNull(host.doneValue) {
		return nil, nil
	}

	var exported any
	if exception := vm.Try(func() { exported = host.doneValue.Export() }); exception != nil {
		return nil, fmt.Errorf("export $done result: %w", exception)
	}
	patch, ok := exported.(map[string]any)
	if !ok {
		return nil, errors.New("$done result must be an object")
	}
	return patch, nil
}

func evaluationError(ctx context.Context, timeout time.Duration, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("timed out after %s", timeout)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return err
}

func (h *runtimeHost) installGlobals(input evaluationInput) error {
	if err := h.vm.Set("$argument", h.entry.argument); err != nil {
		return err
	}
	if err := h.vm.Set("$done", func(call sobek.FunctionCall) sobek.Value {
		if h.doneCalled {
			return sobek.Undefined()
		}
		h.doneCalled = true
		if len(call.Arguments) != 0 {
			h.doneProvided = true
			h.doneValue = call.Argument(0)
		}
		return sobek.Undefined()
	}); err != nil {
		return err
	}

	scriptObject := h.vm.NewObject()
	_ = scriptObject.Set("name", h.entry.name)
	_ = scriptObject.Set("type", string(h.entry.scriptType))
	_ = scriptObject.Set("binaryBodyMode", h.entry.binaryBodyMode)
	startTime, err := h.vm.New(h.vm.Get("Date"), h.vm.ToValue(h.startedAt.UnixMilli()))
	if err != nil {
		return err
	}
	_ = scriptObject.Set("startTime", startTime)
	if err := h.vm.Set("$script", scriptObject); err != nil {
		return err
	}
	if h.entry.scriptType == TypeCron {
		if err := h.vm.Set("$cronexp", h.entry.cronExpression); err != nil {
			return err
		}
	}

	if input.request != nil {
		if err := h.vm.Set("$request", h.requestObject(input)); err != nil {
			return err
		}
	}
	if input.response != nil {
		if err := h.vm.Set("$response", h.responseObject(input)); err != nil {
			return err
		}
	}
	if err := h.vm.Set("$persistentStore", h.persistentStoreObject()); err != nil {
		return err
	}
	if err := h.vm.Set("$httpClient", h.httpClientObject()); err != nil {
		return err
	}
	if err := h.installTimerGlobals(); err != nil {
		return err
	}
	return h.vm.Set("console", h.consoleObject())
}

func (h *runtimeHost) requestObject(input evaluationInput) *sobek.Object {
	request := input.request
	object := h.vm.NewObject()
	_ = object.Set("url", absoluteRequestURL(request))
	_ = object.Set("method", request.Method)
	_ = object.Set("headers", h.headersObject(request.Header, request.Host))
	_ = object.Set("id", input.requestID)
	if input.response == nil && input.bodyPresent {
		_ = object.Set("body", h.bodyValue(input.body))
	}
	return object
}

func (h *runtimeHost) responseObject(input evaluationInput) *sobek.Object {
	response := input.response
	object := h.vm.NewObject()
	_ = object.Set("status", response.StatusCode)
	_ = object.Set("headers", h.headersObject(response.Header, ""))
	if input.bodyPresent {
		_ = object.Set("body", h.bodyValue(input.body))
	}
	return object
}

func (h *runtimeHost) headersObject(headers http.Header, host string) *sobek.Object {
	object := h.vm.NewObject()
	for field, values := range headers {
		_ = object.Set(field, strings.Join(values, ", "))
	}
	if host != "" {
		hostValue := object.Get("Host")
		if hostValue == nil || sobek.IsUndefined(hostValue) {
			_ = object.Set("Host", host)
		}
	}
	return object
}

func (h *runtimeHost) bodyValue(body []byte) sobek.Value {
	if !h.entry.binaryBodyMode {
		return h.vm.ToValue(string(body))
	}
	content := append([]byte(nil), body...)
	arrayBuffer := h.vm.NewArrayBuffer(content)
	array, err := h.vm.New(h.vm.Get("Uint8Array"), h.vm.ToValue(arrayBuffer))
	if err != nil {
		panic(err)
	}
	return array
}

func (h *runtimeHost) persistentStoreObject() *sobek.Object {
	object := h.vm.NewObject()
	_ = object.Set("read", func(call sobek.FunctionCall) sobek.Value {
		key, ok := h.persistentKey(call, 0)
		if !ok || key == "" || len(key) > persistentStoreKeyLimit {
			return sobek.Null()
		}
		data := cachefile.Cache().GetStorage(key)
		if data == nil {
			return sobek.Null()
		}
		return h.vm.ToValue(string(data))
	})
	_ = object.Set("write", func(call sobek.FunctionCall) sobek.Value {
		data, ok := call.Argument(0).Export().(string)
		if !ok || len(data) > persistentStoreDataLimit {
			return h.vm.ToValue(false)
		}
		key, ok := h.persistentKey(call, 1)
		if !ok || key == "" || len(key) > persistentStoreKeyLimit {
			return h.vm.ToValue(false)
		}
		cache := cachefile.Cache()
		if cache.DB == nil {
			return h.vm.ToValue(false)
		}
		cache.SetStorage(key, []byte(data))
		return h.vm.ToValue(true)
	})
	return object
}

func (h *runtimeHost) persistentKey(call sobek.FunctionCall, index int) (string, bool) {
	if len(call.Arguments) <= index || sobek.IsUndefined(call.Argument(index)) || sobek.IsNull(call.Argument(index)) {
		return "script:" + utils.MakeHash([]byte(h.entry.path)).String(), true
	}
	key, ok := call.Argument(index).Export().(string)
	return key, ok
}

func (h *runtimeHost) consoleObject() *sobek.Object {
	object := h.vm.NewObject()
	logger := func(level string) func(sobek.FunctionCall) sobek.Value {
		return func(call sobek.FunctionCall) sobek.Value {
			parts := make([]string, 0, len(call.Arguments))
			for _, argument := range call.Arguments {
				parts = append(parts, argument.String())
			}
			message := strings.Join(parts, " ")
			switch level {
			case "error":
				log.Errorln("[Script] %s: %s", h.entry.name, message)
			case "warn":
				log.Warnln("[Script] %s: %s", h.entry.name, message)
			default:
				if h.entry.debug {
					log.Infoln("[Script] %s: %s", h.entry.name, message)
				} else {
					log.Debugln("[Script] %s: %s", h.entry.name, message)
				}
			}
			return sobek.Undefined()
		}
	}
	_ = object.Set("log", logger("log"))
	_ = object.Set("info", logger("info"))
	_ = object.Set("warn", logger("warn"))
	_ = object.Set("error", logger("error"))
	return object
}

func (m *Manager) runCron(scriptEntry *entry) {
	_, err := m.evaluate(scriptEntry, evaluationInput{})
	if err != nil && m.ctx.Err() == nil {
		log.Errorln("[Script] %s cron execution error: %s", scriptEntry.name, err.Error())
	}
}
