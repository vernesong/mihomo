package common

import (
	"errors"
	"strings"

	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/exp/slices"
)

var (
	errPayload = errors.New("payloadRule error")
)

// params
var (
	NoResolve = "no-resolve"
	Src       = "src"
)

type Base struct {
}

func (b *Base) ProviderNames() []string { return nil }

func ParseParams(params []string) (isSrc bool, noResolve bool) {
	isSrc = slices.Contains(params, Src)
	if isSrc {
		noResolve = true
	} else {
		noResolve = slices.Contains(params, NoResolve)
	}
	return
}

func ParseRulePayload(ruleRaw string) (string, string, []string) {
	item := strings.Split(ruleRaw, ",")
	if len(item) == 1 {
		return "", item[0], nil
	} else if len(item) == 2 {
		return item[0], item[1], nil
	} else if len(item) > 2 {
		// keep in sync with config/config.go [parseRules]
		if item[0] == "NOT" || item[0] == "OR" || item[0] == "AND" || item[0] == "SUB-RULE" || item[0] == "DOMAIN-REGEX" || item[0] == "PROCESS-NAME-REGEX" || item[0] == "PROCESS-PATH-REGEX" {
			return item[0], strings.Join(item[1:], ","), nil
		} else {
			return item[0], item[1], item[2:]
		}
	}

	return "", "", nil
}

type ParseRuleFunc func(tp, payload, target string, params []string, subRules map[string][]C.Rule) (C.Rule, error)
