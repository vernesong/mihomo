package config

import (
	"fmt"

	R "github.com/metacubex/mihomo/component/rewrite"
	"gopkg.in/yaml.v3"
)

type RawRewrite struct {
	URL    []RawRewriteURLRule    `yaml:"url" json:"url"`
	Header []RawRewriteHeaderRule `yaml:"header" json:"header"`
	Body   []RawRewriteBodyRule   `yaml:"body" json:"body"`
	Mock   []RawRewriteMockRule   `yaml:"mock" json:"mock"`
}

type RawRewriteURLRule struct {
	Match string `yaml:"match" json:"match"`
	Type  string `yaml:"type" json:"type"`
	Value string `yaml:"value" json:"value"`
}

type RawRewriteHeaderRule struct {
	Match     string `yaml:"match" json:"match"`
	Direction string `yaml:"direction" json:"direction"`
	Type      string `yaml:"type" json:"type"`
	Field     string `yaml:"field" json:"field"`
	Value     string `yaml:"value" json:"value"`
	Regex     string `yaml:"regex" json:"regex"`
}

type RawRewriteBodyAction struct {
	Regex string `yaml:"regex" json:"regex"`
	Value string `yaml:"value" json:"value"`
}

type RawRewriteBodyRule struct {
	Match        string                 `yaml:"match" json:"match"`
	Direction    string                 `yaml:"direction" json:"direction"`
	Actions      []RawRewriteBodyAction `yaml:"actions" json:"actions"`
	JQExpression *string                `yaml:"jq-expression" json:"jq-expression"`
}

type RawRewriteMockRule struct {
	Match      string            `yaml:"match" json:"match"`
	StatusCode int               `yaml:"status-code" json:"status-code"`
	Headers    RawRewriteHeaders `yaml:"headers" json:"headers"`
	Text       *string           `yaml:"text" json:"text"`
	Base64     *string           `yaml:"base64" json:"base64"`
}

type RawRewriteHeader struct {
	Field string
	Value string
}

type RawRewriteHeaders []RawRewriteHeader

func (h *RawRewriteHeaders) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Kind == 0 || node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		*h = nil
		return nil
	}

	var headers []RawRewriteHeader
	appendMapping := func(mapping *yaml.Node) error {
		if mapping.Kind != yaml.MappingNode {
			return fmt.Errorf("mock headers entries must be mappings")
		}
		for index := 0; index < len(mapping.Content); index += 2 {
			field := mapping.Content[index]
			value := mapping.Content[index+1]
			if field.Kind != yaml.ScalarNode || field.Value == "" {
				return fmt.Errorf("mock header field must be a non-empty string")
			}
			switch value.Kind {
			case yaml.ScalarNode:
				headers = append(headers, RawRewriteHeader{Field: field.Value, Value: value.Value})
			case yaml.SequenceNode:
				for _, item := range value.Content {
					if item.Kind != yaml.ScalarNode {
						return fmt.Errorf("mock header %q values must be strings", field.Value)
					}
					headers = append(headers, RawRewriteHeader{Field: field.Value, Value: item.Value})
				}
			default:
				return fmt.Errorf("mock header %q value must be a string or list", field.Value)
			}
		}
		return nil
	}

	switch node.Kind {
	case yaml.MappingNode:
		if err := appendMapping(node); err != nil {
			return err
		}
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if err := appendMapping(item); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("mock headers must be a mapping or list of mappings")
	}
	*h = headers
	return nil
}

func parseRewrite(raw *RawRewrite) (*Rewrite, error) {
	if raw == nil {
		return nil, nil
	}
	options := R.Options{
		URL:    make([]R.URLRuleOption, 0, len(raw.URL)),
		Header: make([]R.HeaderRuleOption, 0, len(raw.Header)),
		Body:   make([]R.BodyRuleOption, 0, len(raw.Body)),
		Mock:   make([]R.MockRuleOption, 0, len(raw.Mock)),
	}
	for _, rule := range raw.URL {
		options.URL = append(options.URL, R.URLRuleOption{
			Match: rule.Match,
			Type:  rule.Type,
			Value: rule.Value,
		})
	}
	for _, rule := range raw.Header {
		options.Header = append(options.Header, R.HeaderRuleOption{
			Match:     rule.Match,
			Direction: rule.Direction,
			Type:      rule.Type,
			Field:     rule.Field,
			Value:     rule.Value,
			Regex:     rule.Regex,
		})
	}
	for _, rule := range raw.Body {
		bodyRule := R.BodyRuleOption{
			Match:        rule.Match,
			Direction:    rule.Direction,
			JQExpression: rule.JQExpression,
			Actions:      make([]R.BodyActionOption, 0, len(rule.Actions)),
		}
		for _, action := range rule.Actions {
			bodyRule.Actions = append(bodyRule.Actions, R.BodyActionOption{
				Regex: action.Regex,
				Value: action.Value,
			})
		}
		options.Body = append(options.Body, bodyRule)
	}
	for _, rule := range raw.Mock {
		mockRule := R.MockRuleOption{
			Match:      rule.Match,
			StatusCode: rule.StatusCode,
			Text:       rule.Text,
			Base64:     rule.Base64,
			Headers:    make([]R.HeaderValue, 0, len(rule.Headers)),
		}
		for _, header := range rule.Headers {
			mockRule.Headers = append(mockRule.Headers, R.HeaderValue{
				Field: header.Field,
				Value: header.Value,
			})
		}
		options.Mock = append(options.Mock, mockRule)
	}
	return R.NewConfig(options)
}
