package config

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/orderedmap"
	S "github.com/metacubex/mihomo/component/script"
)

const (
	defaultScriptTimeoutSeconds = int64(5)
	defaultScriptMaxBodyKiB     = int64(1024)
)

type RawScript struct {
	Enable   *bool            `yaml:"enable" json:"enable"`
	Debug    bool             `yaml:"debug" json:"debug"`
	Type     string           `yaml:"type" json:"type"`
	Match    string           `yaml:"match" json:"match"`
	Cron     string           `yaml:"cron" json:"cron"`
	Path     string           `yaml:"path" json:"path"`
	URL      string           `yaml:"url" json:"url"`
	Interval int64            `yaml:"interval" json:"interval"`
	Options  RawScriptOptions `yaml:"options" json:"options"`
	Argument string           `yaml:"argument" json:"argument"`
}

type RawScriptOptions struct {
	Timeout        *int64 `yaml:"timeout" json:"timeout"`
	BinaryBodyMode bool   `yaml:"binary-body-mode" json:"binary-body-mode"`
	RequiresBody   bool   `yaml:"requires-body" json:"requires-body"`
	MaxBodySize    *int64 `yaml:"max-body-size" json:"max-body-size"`
	IndirectEval   bool   `yaml:"indirect-eval" json:"indirect-eval"`
}

func parseScripts(raw *orderedmap.OrderedMap[string, RawScript]) (*Scripts, error) {
	if raw == nil {
		return nil, nil
	}
	options := make([]S.EntryOption, 0, raw.Len())
	for pair := raw.Oldest(); pair != nil; pair = pair.Next() {
		name := pair.Key
		rawScript := pair.Value
		if name == "" {
			return nil, fmt.Errorf("scripts: script name cannot be empty")
		}
		if rawScript.Enable == nil {
			return nil, fmt.Errorf("scripts.%s: enable is required", name)
		}
		if rawScript.Interval < 0 {
			return nil, fmt.Errorf("scripts.%s: interval cannot be negative", name)
		}
		if rawScript.Interval > math.MaxInt64/int64(time.Second) {
			return nil, fmt.Errorf("scripts.%s: interval is too large", name)
		}

		timeout := defaultScriptTimeoutSeconds
		if rawScript.Options.Timeout != nil {
			timeout = *rawScript.Options.Timeout
		}
		if timeout <= 0 {
			return nil, fmt.Errorf("scripts.%s: options.timeout must be greater than zero", name)
		}
		if timeout > math.MaxInt64/int64(time.Second) {
			return nil, fmt.Errorf("scripts.%s: options.timeout is too large", name)
		}

		maxBodySize := defaultScriptMaxBodyKiB
		if rawScript.Options.MaxBodySize != nil {
			maxBodySize = *rawScript.Options.MaxBodySize
		}
		if maxBodySize < -1 {
			return nil, fmt.Errorf("scripts.%s: options.max-body-size must be -1 or greater", name)
		}
		maxBodyBytes := int64(-1)
		if maxBodySize >= 0 {
			if maxBodySize > math.MaxInt64/1024 {
				return nil, fmt.Errorf("scripts.%s: options.max-body-size is too large", name)
			}
			maxBodyBytes = maxBodySize * 1024
		}

		options = append(options, S.EntryOption{
			Name:           name,
			Enable:         *rawScript.Enable,
			Debug:          rawScript.Debug,
			Type:           S.Type(strings.ToLower(rawScript.Type)),
			Match:          rawScript.Match,
			Cron:           rawScript.Cron,
			Path:           rawScript.Path,
			URL:            rawScript.URL,
			Interval:       time.Duration(rawScript.Interval) * time.Second,
			Timeout:        time.Duration(timeout) * time.Second,
			BinaryBodyMode: rawScript.Options.BinaryBodyMode,
			RequiresBody:   rawScript.Options.RequiresBody,
			MaxBodySize:    maxBodyBytes,
			IndirectEval:   rawScript.Options.IndirectEval,
			Argument:       rawScript.Argument,
		})
	}
	return S.NewConfig(options)
}
