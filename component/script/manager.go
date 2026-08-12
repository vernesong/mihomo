package script

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/resource"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/grafana/sobek"
	"github.com/robfig/cron/v3"
)

type Type string

const (
	TypeHTTPRequest  Type = "http-request"
	TypeHTTPResponse Type = "http-response"
	TypeCron         Type = "cron"
)

type EntryOption struct {
	Name           string
	Enable         bool
	Debug          bool
	Type           Type
	Match          string
	Cron           string
	Path           string
	URL            string
	Interval       time.Duration
	Timeout        time.Duration
	BinaryBodyMode bool
	RequiresBody   bool
	MaxBodySize    int64
	Argument       string
}

type entry struct {
	name           string
	enable         bool
	debug          bool
	scriptType     Type
	match          *regexp.Regexp
	cronExpression string
	cronSchedule   cron.Schedule
	path           string
	interval       time.Duration
	timeout        time.Duration
	binaryBodyMode bool
	requiresBody   bool
	maxBodySize    int64
	argument       string
	vehicle        *resource.HTTPVehicle

	mutex   sync.RWMutex
	program *sobek.Program
	hash    utils.HashType
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	entries         []*entry
	requestEntries  []*entry
	responseEntries []*entry
	cronEntries     []*entry

	lifecycleMutex sync.Mutex
	started        bool
	scheduler      *cron.Cron
	pullWaitGroup  sync.WaitGroup
	closeOnce      sync.Once
}

var cronParser = cron.NewParser(
	cron.SecondOptional |
		cron.Minute |
		cron.Hour |
		cron.Dom |
		cron.Month |
		cron.Dow,
)

func NewManager(options []EntryOption) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		ctx:       ctx,
		cancel:    cancel,
		entries:   make([]*entry, 0, len(options)),
		scheduler: cron.New(cron.WithParser(cronParser), cron.WithLocation(time.Local)),
	}
	usedPaths := make(map[string]string, len(options))
	for index, option := range options {
		scriptEntry, err := newEntry(option)
		if err != nil {
			_ = manager.Close()
			return nil, fmt.Errorf("scripts.%s: %w", displayName(option.Name, index), err)
		}
		pathKey := canonicalPath(scriptEntry.path)
		if previous, found := usedPaths[pathKey]; found {
			_ = manager.Close()
			return nil, fmt.Errorf("scripts.%s: path duplicates script %q: %s", scriptEntry.name, previous, scriptEntry.path)
		}
		usedPaths[pathKey] = scriptEntry.name

		if scriptEntry.enable {
			if err = scriptEntry.loadInitial(manager.ctx); err != nil {
				_ = manager.Close()
				return nil, fmt.Errorf("scripts.%s: %w", scriptEntry.name, err)
			}
		}
		manager.entries = append(manager.entries, scriptEntry)
		switch scriptEntry.scriptType {
		case TypeHTTPRequest:
			manager.requestEntries = append(manager.requestEntries, scriptEntry)
		case TypeHTTPResponse:
			manager.responseEntries = append(manager.responseEntries, scriptEntry)
		case TypeCron:
			manager.cronEntries = append(manager.cronEntries, scriptEntry)
		}
	}
	return manager, nil
}

func displayName(name string, index int) string {
	if name != "" {
		return name
	}
	return fmt.Sprintf("[%d]", index)
}

func newEntry(option EntryOption) (*entry, error) {
	if option.Name == "" {
		return nil, errors.New("name cannot be empty")
	}
	if option.Path == "" && option.URL == "" {
		return nil, errors.New("path or url is required")
	}
	if option.Interval < 0 {
		return nil, errors.New("interval cannot be negative")
	}
	if option.Timeout <= 0 {
		return nil, errors.New("options.timeout must be greater than zero")
	}
	if option.MaxBodySize < -1 {
		return nil, errors.New("options.max-body-size must be -1 or greater")
	}

	scriptEntry := &entry{
		name:           option.Name,
		enable:         option.Enable,
		debug:          option.Debug,
		scriptType:     option.Type,
		cronExpression: option.Cron,
		interval:       option.Interval,
		timeout:        option.Timeout,
		binaryBodyMode: option.BinaryBodyMode,
		requiresBody:   option.RequiresBody,
		maxBodySize:    option.MaxBodySize,
		argument:       option.Argument,
	}

	switch option.Type {
	case TypeHTTPRequest, TypeHTTPResponse:
		if option.Match == "" {
			return nil, errors.New("match is required for HTTP scripts")
		}
		match, err := regexp.Compile(option.Match)
		if err != nil {
			return nil, fmt.Errorf("invalid match expression: %w", err)
		}
		scriptEntry.match = match
	case TypeCron:
		if option.Cron == "" {
			return nil, errors.New("cron is required for cron scripts")
		}
		fields := strings.Fields(option.Cron)
		if len(fields) != 5 && len(fields) != 6 {
			return nil, errors.New("invalid cron expression: must contain 5 or 6 fields")
		}
		if strings.HasPrefix(fields[0], "TZ=") || strings.HasPrefix(fields[0], "CRON_TZ=") {
			return nil, errors.New("invalid cron expression: timezone overrides are not supported; the system timezone is used")
		}
		schedule, err := cronParser.Parse(option.Cron)
		if err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
		scriptEntry.cronSchedule = schedule
	default:
		return nil, fmt.Errorf("invalid type %q", option.Type)
	}

	if option.URL != "" {
		parsedURL, err := url.ParseRequestURI(option.URL)
		if err != nil || parsedURL.Scheme != "http" && parsedURL.Scheme != "https" || parsedURL.Host == "" {
			return nil, fmt.Errorf("invalid url %q", option.URL)
		}
	}

	path := option.Path
	if path == "" {
		path = C.Path.GetPathByHash("scripts", option.URL)
	} else {
		path = C.Path.Resolve(path)
	}
	if !C.Path.IsSafePath(path) {
		return nil, C.Path.ErrNotSafePath(path)
	}
	scriptEntry.path = path
	if option.URL != "" {
		scriptEntry.vehicle = resource.NewHTTPVehicle(option.URL, path, "", nil, resource.DefaultHttpTimeout, 0)
	}
	return scriptEntry, nil
}

func canonicalPath(path string) string {
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}

func (e *entry) loadInitial(ctx context.Context) error {
	if e.vehicle == nil {
		content, err := os.ReadFile(e.path)
		if err != nil {
			return fmt.Errorf("read local script: %w", err)
		}
		return e.install(content, utils.MakeHash(content))
	}

	content, hash, downloadErr := e.vehicle.Read(ctx, utils.HashType{})
	if downloadErr == nil {
		if err := e.compile(content); err == nil {
			if err = e.vehicle.Write(content); err != nil {
				return fmt.Errorf("write script cache: %w", err)
			}
			return e.install(content, hash)
		} else {
			downloadErr = err
		}
	}

	cached, cacheErr := os.ReadFile(e.path)
	if cacheErr == nil {
		if err := e.install(cached, utils.MakeHash(cached)); err == nil {
			log.Warnln("[Script] %s update failed, using cached script: %s", e.name, downloadErr)
			return nil
		} else {
			cacheErr = err
		}
	}
	return fmt.Errorf("download script: %w; cached script unavailable: %v", downloadErr, cacheErr)
}

func (e *entry) compile(content []byte) error {
	_, err := sobek.Compile(e.path, string(content), false)
	if err != nil {
		return fmt.Errorf("compile JavaScript: %w", err)
	}
	return nil
}

func (e *entry) install(content []byte, hash utils.HashType) error {
	program, err := sobek.Compile(e.path, string(content), false)
	if err != nil {
		return fmt.Errorf("compile JavaScript: %w", err)
	}
	e.mutex.Lock()
	e.program = program
	e.hash = hash
	e.mutex.Unlock()
	return nil
}

func (e *entry) currentProgram() *sobek.Program {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.program
}

func (m *Manager) Start() {
	if m == nil {
		return
	}
	m.lifecycleMutex.Lock()
	if m.started || m.ctx.Err() != nil {
		m.lifecycleMutex.Unlock()
		return
	}
	m.started = true
	for _, scriptEntry := range m.cronEntries {
		if !scriptEntry.enable {
			continue
		}
		entryCopy := scriptEntry
		m.scheduler.Schedule(entryCopy.cronSchedule, cron.FuncJob(func() {
			m.runCron(entryCopy)
		}))
	}
	m.scheduler.Start()
	for _, scriptEntry := range m.entries {
		if scriptEntry.enable && scriptEntry.vehicle != nil && scriptEntry.interval > 0 {
			m.pullWaitGroup.Add(1)
			go func() {
				defer m.pullWaitGroup.Done()
				m.pullLoop(scriptEntry)
			}()
		}
	}
	m.lifecycleMutex.Unlock()
}

func (m *Manager) pullLoop(scriptEntry *entry) {
	timer := time.NewTimer(scriptEntry.interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := scriptEntry.refresh(m.ctx); err != nil {
				if m.ctx.Err() != nil {
					return
				}
				log.Errorln("[Script] %s update error: %s", scriptEntry.name, err.Error())
			}
			timer.Reset(scriptEntry.interval)
		case <-m.ctx.Done():
			return
		}
	}
}

func (e *entry) refresh(ctx context.Context) error {
	e.mutex.RLock()
	oldHash := e.hash
	e.mutex.RUnlock()
	content, hash, err := e.vehicle.Read(ctx, oldHash)
	if err != nil {
		return err
	}
	if oldHash.Equal(hash) {
		now := time.Now()
		_ = os.Chtimes(e.path, now, now)
		return nil
	}
	program, err := sobek.Compile(e.path, string(content), false)
	if err != nil {
		return fmt.Errorf("compile JavaScript: %w", err)
	}
	if err = e.vehicle.Write(content); err != nil {
		return fmt.Errorf("write script cache: %w", err)
	}
	e.mutex.Lock()
	e.program = program
	e.hash = hash
	e.mutex.Unlock()
	log.Infoln("[Script] %s's content update", e.name)
	return nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		m.lifecycleMutex.Lock()
		m.cancel()
		started := m.started
		stopped := m.scheduler.Stop()
		m.lifecycleMutex.Unlock()
		if started {
			<-stopped.Done()
		}
		m.pullWaitGroup.Wait()
	})
	return nil
}
