package common

import (
	C "github.com/metacubex/mihomo/constant"

	"github.com/dlclark/regexp2"
)

type URLRegex struct {
	Base
	pattern string
	regex   *regexp2.Regexp
	adapter string
}

func (ur *URLRegex) RuleType() C.RuleType {
	return C.URLRegex
}

func (ur *URLRegex) Match(metadata *C.Metadata, _ C.RuleMatchHelper) (bool, string) {
	if metadata.URL == "" {
		return false, ur.adapter
	}
	matched, _ := ur.regex.MatchString(metadata.URL)
	return matched, ur.adapter
}

func (ur *URLRegex) Adapter() string {
	return ur.adapter
}

func (ur *URLRegex) Payload() string {
	return ur.pattern
}

func NewURLRegex(pattern string, adapter string) (*URLRegex, error) {
	regex, err := regexp2.Compile(pattern, regexp2.None)
	if err != nil {
		return nil, err
	}
	return &URLRegex{
		Base:    Base{},
		pattern: pattern,
		regex:   regex,
		adapter: adapter,
	}, nil
}

var _ C.Rule = (*URLRegex)(nil)
