package modules

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/orderedmap"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/resource"
	"github.com/metacubex/mihomo/component/slowdown"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/fswatch"
	"gopkg.in/yaml.v3"
)

var ErrNotFound = errors.New("module not found")

type rawModule struct {
	Enable   *bool  `yaml:"enable"`
	Type     string `yaml:"type"`
	Path     string `yaml:"path"`
	URL      string `yaml:"url"`
	Interval int    `yaml:"interval"`
}

type Info struct {
	Enable    bool       `json:"enable"`
	Type      string     `json:"type"`
	Path      string     `json:"path"`
	URL       string     `json:"url,omitempty"`
	Interval  int        `json:"interval"`
	UpdatedAt *time.Time `json:"updatedAt,omitempty"`
}

type entry struct {
	name        string
	enable      bool
	moduleType  string
	path        string
	url         string
	interval    time.Duration
	intervalSec int
	vehicle     P.Vehicle

	mu        sync.RWMutex
	content   []byte
	hash      utils.HashType
	updatedAt time.Time
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	entries []*entry
	byName  map[string]*entry
	byPath  map[string]*entry

	sourceMu    sync.RWMutex
	sourcePath  string
	sourceBytes []byte
	effective   []byte

	lifecycleMu sync.Mutex
	refreshMu   sync.Mutex
	started     bool
	onUpdate    func(*Manager) error
	watcher     *fswatch.Watcher
	closeOnce   sync.Once
}

func Parse(source []byte) (*Manager, []byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		ctx:         ctx,
		cancel:      cancel,
		byName:      map[string]*entry{},
		byPath:      map[string]*entry{},
		sourceBytes: append([]byte(nil), source...),
		effective:   append([]byte(nil), source...),
	}

	document, err := parseDocument(source)
	if err != nil {
		manager.Close()
		return nil, nil, err
	}
	root := documentRoot(document)
	if root == nil || root.Kind != yaml.MappingNode {
		return manager, source, nil
	}

	modulesNode, found := getMappingValue(root, "modules")
	if !found {
		return manager, source, nil
	}
	if modulesNode.Kind != yaml.MappingNode {
		manager.Close()
		return nil, nil, fmt.Errorf("modules must be a YAML mapping")
	}

	usedPaths := map[string]string{}
	for index := 0; index < len(modulesNode.Content); index += 2 {
		name, err := mappingKey(modulesNode.Content[index])
		if err != nil {
			manager.Close()
			return nil, nil, fmt.Errorf("modules: %w", err)
		}
		if name == "" {
			manager.Close()
			return nil, nil, fmt.Errorf("module name cannot be empty")
		}
		if _, exists := manager.byName[name]; exists {
			manager.Close()
			return nil, nil, fmt.Errorf("module name %q is duplicated", name)
		}

		raw := rawModule{}
		if err := modulesNode.Content[index+1].Decode(&raw); err != nil {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: %w", name, err)
		}
		if raw.Enable == nil {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: enable is required", name)
		}
		if raw.Interval < 0 {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: interval cannot be negative", name)
		}
		if int64(raw.Interval) > math.MaxInt64/int64(time.Second) {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: interval is too large", name)
		}

		moduleEntry := &entry{
			name:        name,
			enable:      *raw.Enable,
			moduleType:  raw.Type,
			url:         raw.URL,
			interval:    time.Duration(raw.Interval) * time.Second,
			intervalSec: raw.Interval,
		}

		switch raw.Type {
		case "file":
			if raw.Path == "" {
				manager.Close()
				return nil, nil, fmt.Errorf("module %q: path is required for file type", name)
			}
			moduleEntry.path = C.Path.Resolve(raw.Path)
			moduleEntry.vehicle = resource.NewFileVehicle(moduleEntry.path)
		case "http":
			if raw.URL == "" {
				manager.Close()
				return nil, nil, fmt.Errorf("module %q: url is required for http type", name)
			}
			moduleEntry.path = C.Path.GetPathByHash("modules", raw.URL)
			if raw.Path != "" {
				moduleEntry.path = C.Path.Resolve(raw.Path)
			}
			moduleEntry.vehicle = resource.NewHTTPVehicle(raw.URL, moduleEntry.path, "", nil, resource.DefaultHttpTimeout, 0)
		default:
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: unsupported type %q", name, raw.Type)
		}

		if !C.Path.IsSafePath(moduleEntry.path) {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: %w", name, C.Path.ErrNotSafePath(moduleEntry.path))
		}
		pathKey := canonicalPath(moduleEntry.path)
		if previous, exists := usedPaths[pathKey]; exists {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: path duplicates module %q: %s", name, previous, moduleEntry.path)
		}
		usedPaths[pathKey] = name

		manager.entries = append(manager.entries, moduleEntry)
		manager.byName[name] = moduleEntry
		manager.byPath[pathKey] = moduleEntry
	}

	for _, moduleEntry := range manager.entries {
		if !moduleEntry.enable {
			continue
		}
		if err := moduleEntry.loadInitial(manager.ctx); err != nil {
			manager.Close()
			return nil, nil, fmt.Errorf("module %q: %w", moduleEntry.name, err)
		}
	}

	overrides := make([]namedOverride, 0, len(manager.entries))
	for _, moduleEntry := range manager.entries {
		if moduleEntry.enable {
			overrides = append(overrides, namedOverride{name: moduleEntry.name, content: moduleEntry.contentCopy()})
		}
	}
	merged, err := mergeConfig(source, overrides)
	if err != nil {
		manager.Close()
		return nil, nil, err
	}
	manager.effective = append(manager.effective[:0], merged...)

	return manager, merged, nil
}

func canonicalPath(path string) string {
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}

func (e *entry) loadInitial(ctx context.Context) error {
	if e.moduleType == "http" {
		if buf, err := os.ReadFile(e.path); err == nil {
			if validateErr := validateOverride(buf); validateErr == nil {
				updatedAt := time.Now()
				if stat, statErr := os.Stat(e.path); statErr == nil {
					updatedAt = stat.ModTime()
				}
				e.setContent(buf, utils.MakeHash(buf), updatedAt)
				return nil
			}
		}
	}

	buf, hash, err := e.vehicle.Read(ctx, utils.HashType{})
	if err != nil {
		return err
	}
	if err := validateOverride(buf); err != nil {
		return err
	}
	if e.moduleType == "http" {
		if err := e.vehicle.Write(buf); err != nil {
			return err
		}
	}
	updatedAt := time.Now()
	if stat, statErr := os.Stat(e.path); statErr == nil {
		updatedAt = stat.ModTime()
	}
	e.setContent(buf, hash, updatedAt)
	return nil
}

func (e *entry) setContent(content []byte, hash utils.HashType, updatedAt time.Time) {
	e.mu.Lock()
	e.content = append(e.content[:0], content...)
	e.hash = hash
	e.updatedAt = updatedAt
	e.mu.Unlock()
}

func (e *entry) contentCopy() []byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]byte(nil), e.content...)
}

func (m *Manager) SetSourcePath(path string) {
	m.sourceMu.Lock()
	m.sourcePath = path
	m.sourceMu.Unlock()
}

func (m *Manager) SourcePath() string {
	m.sourceMu.RLock()
	defer m.sourceMu.RUnlock()
	return m.sourcePath
}

func (m *Manager) EffectiveConfig() []byte {
	m.sourceMu.RLock()
	defer m.sourceMu.RUnlock()
	return append([]byte(nil), m.effective...)
}

func (m *Manager) ReadSource() ([]byte, error) {
	m.sourceMu.RLock()
	path := m.sourcePath
	buf := append([]byte(nil), m.sourceBytes...)
	m.sourceMu.RUnlock()
	if path != "" {
		return os.ReadFile(path)
	}
	return buf, nil
}

func (m *Manager) Start(onUpdate func(*Manager) error) {
	m.lifecycleMu.Lock()
	if m.started || m.ctx.Err() != nil {
		m.lifecycleMu.Unlock()
		return
	}
	m.started = true
	m.onUpdate = onUpdate

	filePaths := make([]string, 0)
	for _, moduleEntry := range m.entries {
		if moduleEntry.enable && moduleEntry.moduleType == "file" {
			filePaths = append(filePaths, moduleEntry.path)
		}
	}
	m.lifecycleMu.Unlock()

	if len(filePaths) > 0 {
		watcher, err := fswatch.NewWatcher(fswatch.Options{
			Path: filePaths,
			Callback: func(path string) {
				if moduleEntry := m.byPath[canonicalPath(path)]; moduleEntry != nil {
					go m.refresh(moduleEntry)
				}
			},
		})
		if err != nil {
			log.Errorln("[Module] create file watcher error: %s", err.Error())
		} else if err = watcher.Start(); err != nil {
			log.Errorln("[Module] start file watcher error: %s", err.Error())
			_ = watcher.Close()
		} else {
			m.lifecycleMu.Lock()
			if m.ctx.Err() == nil {
				m.watcher = watcher
			} else {
				_ = watcher.Close()
			}
			m.lifecycleMu.Unlock()
		}
	}

	for _, moduleEntry := range m.entries {
		if moduleEntry.enable && moduleEntry.moduleType == "http" && moduleEntry.interval > 0 {
			go m.pullLoop(moduleEntry)
		}
	}
}

func (m *Manager) pullLoop(moduleEntry *entry) {
	moduleEntry.mu.RLock()
	updatedAt := moduleEntry.updatedAt
	moduleEntry.mu.RUnlock()

	delay := moduleEntry.interval
	if !updatedAt.IsZero() {
		elapsed := time.Since(updatedAt)
		if elapsed >= moduleEntry.interval {
			delay = 0
		} else if elapsed >= 0 {
			delay = moduleEntry.interval - elapsed
		}
	}

	minBackoff := 10 * time.Second
	if moduleEntry.interval < minBackoff {
		minBackoff = moduleEntry.interval
	}
	backoff := slowdown.Backoff{Factor: 2, Min: minBackoff, Max: moduleEntry.interval}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			err := m.refresh(moduleEntry)
			next := moduleEntry.interval
			if err != nil {
				backoff.AddAttempt()
				if retry := backoff.ForAttempt(backoff.Attempt()); retry < next {
					next = retry
				}
			} else {
				backoff.Reset()
			}
			timer.Reset(next)
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) refresh(moduleEntry *entry) error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	default:
	}

	moduleEntry.mu.RLock()
	oldHash := moduleEntry.hash
	oldContent := append([]byte(nil), moduleEntry.content...)
	moduleEntry.mu.RUnlock()

	buf, hash, err := moduleEntry.vehicle.Read(m.ctx, oldHash)
	if err != nil {
		log.Errorln("[Module] %s update error: %s", moduleEntry.name, err.Error())
		return err
	}
	if oldHash.Equal(hash) {
		now := time.Now()
		if moduleEntry.moduleType == "http" {
			_ = os.Chtimes(moduleEntry.path, now, now)
		}
		moduleEntry.mu.Lock()
		moduleEntry.updatedAt = now
		moduleEntry.mu.Unlock()
		log.Debugln("[Module] %s's content doesn't change", moduleEntry.name)
		return nil
	}
	if err := validateOverride(buf); err != nil {
		log.Errorln("[Module] %s update error: %s", moduleEntry.name, err.Error())
		return err
	}

	if moduleEntry.moduleType == "http" {
		if err := moduleEntry.vehicle.Write(buf); err != nil {
			log.Errorln("[Module] %s cache write error: %s", moduleEntry.name, err.Error())
			return err
		}
	}

	m.lifecycleMu.Lock()
	onUpdate := m.onUpdate
	m.lifecycleMu.Unlock()
	if onUpdate != nil {
		if err := onUpdate(m); err != nil {
			if moduleEntry.moduleType == "http" {
				if rollbackErr := moduleEntry.vehicle.Write(oldContent); rollbackErr != nil {
					log.Errorln("[Module] %s cache rollback error: %s", moduleEntry.name, rollbackErr.Error())
				}
			}
			log.Errorln("[Module] %s reload error: %s", moduleEntry.name, err.Error())
			return err
		}
	}

	moduleEntry.setContent(buf, hash, time.Now())
	log.Infoln("[Module] %s's content update", moduleEntry.name)
	return nil
}

func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		m.cancel()
		m.lifecycleMu.Lock()
		watcher := m.watcher
		m.watcher = nil
		m.lifecycleMu.Unlock()
		if watcher != nil {
			err = watcher.Close()
		}
	})
	return err
}

func (m *Manager) Snapshot() *orderedmap.OrderedMap[string, Info] {
	snapshot := orderedmap.New[string, Info](len(m.entries))
	for _, moduleEntry := range m.entries {
		snapshot.Set(moduleEntry.name, moduleEntry.info())
	}
	return snapshot
}

func (m *Manager) Get(name string) (Info, bool) {
	moduleEntry, found := m.byName[name]
	if !found {
		return Info{}, false
	}
	return moduleEntry.info(), true
}

func (e *entry) info() Info {
	e.mu.RLock()
	defer e.mu.RUnlock()
	info := Info{
		Enable:   e.enable,
		Type:     e.moduleType,
		Path:     e.path,
		URL:      e.url,
		Interval: e.intervalSec,
	}
	if !e.updatedAt.IsZero() {
		updatedAt := e.updatedAt
		info.UpdatedAt = &updatedAt
	}
	return info
}
