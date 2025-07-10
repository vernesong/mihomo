package provider

import (
	"fmt"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/rules/common"
)

type classicalStrategy struct {
	rules []C.Rule
	count int
	parse func(tp, payload, target string, params []string) (parsed C.Rule, parseErr error)
}

func (c *classicalStrategy) Behavior() P.RuleBehavior {
	return P.Classical
}

func (c *classicalStrategy) Match(metadata *C.Metadata, helper C.RuleMatchHelper) bool {
	for _, rule := range c.rules {
		if m, _ := rule.Match(metadata, helper); m {
			return true
		}
	}

	return false
}

func (c *classicalStrategy) Count() int {
	return c.count
}

func (c *classicalStrategy) Reset() {
	c.rules = nil
	c.count = 0
}

func (c *classicalStrategy) Insert(rule string) {
	ruleType, rule, params := common.ParseRulePayload(rule)
	r, err := c.parse(ruleType, rule, "", params)
	if err != nil {
		log.Warnln("parse classical rule error: %s", err.Error())
	} else {
		c.rules = append(c.rules, r)
		c.count++
	}
}

func (c *classicalStrategy) FinishInsert() {}

func NewClassicalStrategy(parse func(tp, payload, target string, params []string, subRules map[string][]C.Rule) (parsed C.Rule, parseErr error)) *classicalStrategy {
	return &classicalStrategy{rules: []C.Rule{}, parse: func(tp, payload, target string, params []string) (parsed C.Rule, parseErr error) {
		switch tp {
		case "MATCH", "RULE-SET", "SUB-RULE":
			return nil, fmt.Errorf("unsupported rule type on classical rule-set: %s", tp)
		default:
			return parse(tp, payload, target, params, nil)
		}
	}}
}
