package mitm

import (
	"fmt"
	"strconv"
	"strings"

	F "github.com/metacubex/mihomo/component/httpflow"
	"github.com/metacubex/mihomo/log"
)

func logTransactionAction(transactionID string, action F.Action) {
	fields := []log.Field{
		{Key: "transactionId", Value: transactionID},
		{Key: "actionIndex", Value: strconv.FormatUint(action.Index, 10)},
		{Key: "phase", Value: string(action.Phase)},
		{Key: "source", Value: string(action.Source)},
		{Key: "kind", Value: string(action.Kind)},
		{Key: "outcome", Value: string(action.Outcome)},
		{Key: "modified", Value: strconv.FormatBool(action.Modified)},
	}
	if action.Name != "" {
		fields = append(fields, log.Field{Key: "name", Value: action.Name})
	}
	if action.Rule != "" {
		fields = append(fields, log.Field{Key: "rule", Value: action.Rule})
	}
	if action.StatusCode != 0 {
		fields = append(fields, log.Field{Key: "statusCode", Value: strconv.Itoa(action.StatusCode)})
	}
	if action.Target != "" {
		fields = append(fields, log.Field{Key: "target", Value: action.Target})
	}
	if len(action.Fields) != 0 {
		fields = append(fields, log.Field{Key: "fields", Value: strings.Join(action.Fields, ",")})
	}

	message := fmt.Sprintf("[HTTP] %s %s %s %s %s", transactionID, action.Phase, action.Source, action.Kind, action.Outcome)
	if action.Name != "" {
		message += " " + action.Name
	}
	if action.StatusCode != 0 {
		message += " status=" + strconv.Itoa(action.StatusCode)
	}
	if action.Target != "" {
		message += " target=" + action.Target
	}
	if action.Message != "" {
		message += ": " + action.Message
	}

	switch action.Outcome {
	case F.OutcomeFailed:
		log.ErrorFields(fields, "%s", message)
	case F.OutcomeSkipped, F.OutcomeUnchanged:
		log.DebugFields(fields, "%s", message)
	default:
		log.InfoFields(fields, "%s", message)
	}
}

func logTransactionFailure(transactionID, stage string, source F.Source, err error) {
	if err == nil {
		return
	}
	fields := []log.Field{
		{Key: "transactionId", Value: transactionID},
		{Key: "state", Value: string(F.StateFailed)},
		{Key: "stage", Value: stage},
		{Key: "source", Value: string(source)},
	}
	log.WarnFields(fields, "[HTTP] %s failed at %s: %s", transactionID, stage, err.Error())
}
