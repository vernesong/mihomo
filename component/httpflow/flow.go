package httpflow

import (
	"time"

	"github.com/metacubex/http"
)

type State string

const (
	StateActive    State = "active"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateAborted   State = "aborted"
	StateCancelled State = "cancelled"
)

type Phase string

const (
	PhaseRequest  Phase = "request"
	PhaseResponse Phase = "response"
)

type Source string

const (
	SourceRewrite  Source = "rewrite"
	SourceScript   Source = "script"
	SourceHandler  Source = "handler"
	SourceUpstream Source = "upstream"
	SourceClient   Source = "client"
)

type Kind string

const (
	KindURL           Kind = "url"
	KindHeader        Kind = "header"
	KindBody          Kind = "body"
	KindRedirect      Kind = "redirect"
	KindReject        Kind = "reject"
	KindMock          Kind = "mock"
	KindScript        Kind = "script"
	KindLocalResponse Kind = "local-response"
	KindRequest       Kind = "request"
	KindResponse      Kind = "response"
)

type Outcome string

const (
	OutcomeApplied   Outcome = "applied"
	OutcomeResponded Outcome = "responded"
	OutcomeAborted   Outcome = "aborted"
	OutcomeUnchanged Outcome = "unchanged"
	OutcomeSkipped   Outcome = "skipped"
	OutcomeFailed    Outcome = "failed"
)

type Action struct {
	Index      uint64    `json:"index"`
	At         time.Time `json:"at"`
	Phase      Phase     `json:"phase"`
	Source     Source    `json:"source"`
	Kind       Kind      `json:"kind"`
	Outcome    Outcome   `json:"outcome"`
	Name       string    `json:"name,omitempty"`
	Rule       string    `json:"rule,omitempty"`
	Modified   bool      `json:"modified"`
	StatusCode int       `json:"statusCode,omitempty"`
	Target     string    `json:"target,omitempty"`
	Fields     []string  `json:"fields,omitempty"`
	Message    string    `json:"message,omitempty"`
}

type Failure struct {
	At      time.Time `json:"at"`
	Stage   string    `json:"stage"`
	Source  Source    `json:"source"`
	Message string    `json:"message"`
}

type Decision uint8

const (
	DecisionContinue Decision = iota
	DecisionRespond
	DecisionAbort
)

type Result struct {
	Decision Decision
	Response *http.Response
	Actions  []Action
}

func (r *Result) Add(action Action) {
	r.Actions = append(r.Actions, action)
}
