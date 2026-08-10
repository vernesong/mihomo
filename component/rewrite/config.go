package rewrite

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"github.com/itchyny/gojq"
	"golang.org/x/net/http/httpguts"
)

type URLRuleOption struct {
	Match string
	Type  string
	Value string
}

type HeaderRuleOption struct {
	Match     string
	Direction string
	Type      string
	Field     string
	Value     string
	Regex     string
}

type BodyActionOption struct {
	Regex string
	Value string
}

type BodyRuleOption struct {
	Match        string
	Direction    string
	Actions      []BodyActionOption
	JQExpression *string
}

type HeaderValue struct {
	Field string
	Value string
}

type MockRuleOption struct {
	Match      string
	StatusCode int
	Headers    []HeaderValue
	Text       *string
	Base64     *string
}

type Options struct {
	URL    []URLRuleOption
	Header []HeaderRuleOption
	Body   []BodyRuleOption
	Mock   []MockRuleOption
}

type Config struct {
	urlRules            []urlRule
	requestHeaderRules  []headerRule
	responseHeaderRules []headerRule
	requestBodyRules    []bodyRule
	responseBodyRules   []bodyRule
	mockRules           []mockRule
}

type direction uint8

const (
	directionRequest direction = iota + 1
	directionResponse
)

type urlRuleType uint8

const (
	urlRuleTransparent urlRuleType = iota + 1
	urlRuleRedirect302
	urlRuleRedirect307
	urlRuleReject
)

type headerRuleType uint8

const (
	headerRuleAdd headerRuleType = iota + 1
	headerRuleDelete
	headerRuleReplace
	headerRuleReplaceRegex
)

type rejectResponseType uint8

const (
	rejectResponseNotFound rejectResponseType = iota + 1
	rejectResponseOK
	rejectResponseImage
	rejectResponseDict
	rejectResponseArray
)

type urlRule struct {
	match      *regexp.Regexp
	ruleType   urlRuleType
	value      string
	rejectType rejectResponseType
}

type headerRule struct {
	match     *regexp.Regexp
	direction direction
	ruleType  headerRuleType
	field     string
	value     string
	regex     *regexp.Regexp
}

type bodyAction struct {
	regex *regexp.Regexp
	value []byte
}

type bodyRule struct {
	match        *regexp.Regexp
	direction    direction
	actions      []bodyAction
	jqExpression *gojq.Code
}

type mockRule struct {
	match      *regexp.Regexp
	statusCode int
	headers    []HeaderValue
	body       []byte
}

func NewConfig(options Options) (*Config, error) {
	config := &Config{
		urlRules:  make([]urlRule, 0, len(options.URL)),
		mockRules: make([]mockRule, 0, len(options.Mock)),
	}
	for index, option := range options.URL {
		rule, err := newURLRule(option)
		if err != nil {
			return nil, fmt.Errorf("rewrite.url[%d]: %w", index, err)
		}
		config.urlRules = append(config.urlRules, rule)
	}
	for index, option := range options.Header {
		rule, err := newHeaderRule(option)
		if err != nil {
			return nil, fmt.Errorf("rewrite.header[%d]: %w", index, err)
		}
		if rule.direction == directionRequest {
			config.requestHeaderRules = append(config.requestHeaderRules, rule)
		} else {
			config.responseHeaderRules = append(config.responseHeaderRules, rule)
		}
	}
	for index, option := range options.Body {
		rule, err := newBodyRule(option)
		if err != nil {
			return nil, fmt.Errorf("rewrite.body[%d]: %w", index, err)
		}
		if rule.direction == directionRequest {
			config.requestBodyRules = append(config.requestBodyRules, rule)
		} else {
			config.responseBodyRules = append(config.responseBodyRules, rule)
		}
	}
	for index, option := range options.Mock {
		rule, err := newMockRule(option)
		if err != nil {
			return nil, fmt.Errorf("rewrite.mock[%d]: %w", index, err)
		}
		config.mockRules = append(config.mockRules, rule)
	}
	return config, nil
}

func newURLRule(option URLRuleOption) (urlRule, error) {
	match, err := regexp.Compile(option.Match)
	if err != nil {
		return urlRule{}, fmt.Errorf("invalid match expression: %w", err)
	}
	if !httpguts.ValidHeaderFieldValue(option.Value) {
		return urlRule{}, fmt.Errorf("invalid value %q", option.Value)
	}
	rule := urlRule{match: match, value: option.Value}
	switch strings.ToLower(option.Type) {
	case "transparent":
		if option.Value == "" {
			return urlRule{}, fmt.Errorf("value is required for transparent")
		}
		rule.ruleType = urlRuleTransparent
	case "redirect-302":
		if option.Value == "" {
			return urlRule{}, fmt.Errorf("value is required for redirect-302")
		}
		rule.ruleType = urlRuleRedirect302
	case "redirect-307":
		if option.Value == "" {
			return urlRule{}, fmt.Errorf("value is required for redirect-307")
		}
		rule.ruleType = urlRuleRedirect307
	case "reject":
		rule.ruleType = urlRuleReject
		switch strings.ToLower(option.Value) {
		case "":
			rule.rejectType = rejectResponseNotFound
		case "200":
			rule.rejectType = rejectResponseOK
		case "img":
			rule.rejectType = rejectResponseImage
		case "dict":
			rule.rejectType = rejectResponseDict
		case "array":
			rule.rejectType = rejectResponseArray
		default:
			return urlRule{}, fmt.Errorf("invalid reject value %q", option.Value)
		}
	default:
		return urlRule{}, fmt.Errorf("invalid type %q", option.Type)
	}
	return rule, nil
}

func newHeaderRule(option HeaderRuleOption) (headerRule, error) {
	match, err := regexp.Compile(option.Match)
	if err != nil {
		return headerRule{}, fmt.Errorf("invalid match expression: %w", err)
	}
	directionValue, err := parseDirection(option.Direction)
	if err != nil {
		return headerRule{}, err
	}
	if !httpguts.ValidHeaderFieldName(option.Field) {
		return headerRule{}, fmt.Errorf("invalid field %q", option.Field)
	}
	if !httpguts.ValidHeaderFieldValue(option.Value) {
		return headerRule{}, fmt.Errorf("invalid value for field %q", option.Field)
	}

	rule := headerRule{
		match:     match,
		direction: directionValue,
		field:     option.Field,
		value:     option.Value,
	}
	switch strings.ToLower(option.Type) {
	case "add":
		rule.ruleType = headerRuleAdd
	case "del":
		rule.ruleType = headerRuleDelete
	case "replace":
		rule.ruleType = headerRuleReplace
	case "replace-regex":
		rule.ruleType = headerRuleReplaceRegex
		rule.regex, err = regexp.Compile(option.Regex)
		if err != nil {
			return headerRule{}, fmt.Errorf("invalid regex expression: %w", err)
		}
	default:
		return headerRule{}, fmt.Errorf("invalid type %q", option.Type)
	}
	return rule, nil
}

func newBodyRule(option BodyRuleOption) (bodyRule, error) {
	match, err := regexp.Compile(option.Match)
	if err != nil {
		return bodyRule{}, fmt.Errorf("invalid match expression: %w", err)
	}
	directionValue, err := parseDirection(option.Direction)
	if err != nil {
		return bodyRule{}, err
	}
	if len(option.Actions) == 0 && option.JQExpression == nil {
		return bodyRule{}, fmt.Errorf("actions or jq-expression is required")
	}
	if len(option.Actions) != 0 && option.JQExpression != nil {
		return bodyRule{}, fmt.Errorf("actions and jq-expression are mutually exclusive")
	}

	rule := bodyRule{match: match, direction: directionValue}
	for index, option := range option.Actions {
		expression, err := regexp.Compile(option.Regex)
		if err != nil {
			return bodyRule{}, fmt.Errorf("actions[%d]: invalid regex expression: %w", index, err)
		}
		rule.actions = append(rule.actions, bodyAction{regex: expression, value: []byte(option.Value)})
	}
	if option.JQExpression != nil {
		query, err := gojq.Parse(*option.JQExpression)
		if err != nil {
			return bodyRule{}, fmt.Errorf("invalid jq-expression: %w", err)
		}
		rule.jqExpression, err = gojq.Compile(query)
		if err != nil {
			return bodyRule{}, fmt.Errorf("compile jq-expression: %w", err)
		}
	}
	return rule, nil
}

func newMockRule(option MockRuleOption) (mockRule, error) {
	match, err := regexp.Compile(option.Match)
	if err != nil {
		return mockRule{}, fmt.Errorf("invalid match expression: %w", err)
	}
	if option.Text != nil && option.Base64 != nil {
		return mockRule{}, fmt.Errorf("text and base64 are mutually exclusive")
	}
	statusCode := option.StatusCode
	if statusCode == 0 {
		statusCode = 200
	}
	if statusCode < 100 || statusCode > 599 {
		return mockRule{}, fmt.Errorf("invalid status-code %d", statusCode)
	}

	var body []byte
	if option.Text != nil {
		body = []byte(*option.Text)
	}
	if option.Base64 != nil {
		body, err = base64.StdEncoding.DecodeString(*option.Base64)
		if err != nil {
			return mockRule{}, fmt.Errorf("invalid base64 body: %w", err)
		}
	}
	for _, header := range option.Headers {
		if !httpguts.ValidHeaderFieldName(header.Field) {
			return mockRule{}, fmt.Errorf("invalid header field %q", header.Field)
		}
		if !httpguts.ValidHeaderFieldValue(header.Value) {
			return mockRule{}, fmt.Errorf("invalid value for header field %q", header.Field)
		}
	}
	return mockRule{
		match:      match,
		statusCode: statusCode,
		headers:    append([]HeaderValue(nil), option.Headers...),
		body:       body,
	}, nil
}

func parseDirection(value string) (direction, error) {
	switch strings.ToLower(value) {
	case "request":
		return directionRequest, nil
	case "response":
		return directionResponse, nil
	default:
		return 0, fmt.Errorf("invalid direction %q", value)
	}
}
