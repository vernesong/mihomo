package script

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/grafana/sobek"
	jsAST "github.com/grafana/sobek/ast"
)

type sourceSpan struct {
	start int
	end   int
}

func (e *entrySpec) compileProgram(content []byte) (*sobek.Program, error) {
	source := string(content)
	if e.indirectEval {
		var err error
		source, err = rewriteDirectEvalCalls(e.path, source)
		if err != nil {
			return nil, fmt.Errorf("compile JavaScript: %w", err)
		}
	}
	program, err := sobek.Compile(e.path, source, false)
	if err != nil {
		return nil, fmt.Errorf("compile JavaScript: %w", err)
	}
	return program, nil
}

func rewriteDirectEvalCalls(name, source string) (string, error) {
	program, err := sobek.Parse(name, source)
	if err != nil {
		return "", err
	}

	spansByStart := make(map[int]int)
	walkScriptAST(reflect.ValueOf(program), make(map[astPointer]struct{}), func(value any) {
		call, ok := value.(*jsAST.CallExpression)
		if !ok {
			return
		}
		identifier, ok := call.Callee.(*jsAST.Identifier)
		if !ok || identifier.Name.String() != "eval" {
			return
		}
		start := int(identifier.Idx0()) - 1
		end := int(call.LeftParenthesis) - 1
		if previousEnd, found := spansByStart[start]; !found || end > previousEnd {
			spansByStart[start] = end
		}
	})

	spans := make([]sourceSpan, 0, len(spansByStart))
	for start, end := range spansByStart {
		if start < 0 || end < start || end > len(source) {
			return "", fmt.Errorf("invalid direct eval source span [%d:%d]", start, end)
		}
		spans = append(spans, sourceSpan{start: start, end: end})
	}
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].start > spans[j].start
	})
	for _, span := range spans {
		source = source[:span.end] + ")" + source[span.end:]
		source = source[:span.start] + "(0," + source[span.start:]
	}
	return source, nil
}

type astPointer struct {
	typeOf reflect.Type
	value  uintptr
}

var scriptASTPackage = reflect.TypeOf(jsAST.Program{}).PkgPath()

func walkScriptAST(value reflect.Value, visited map[astPointer]struct{}, visit func(any)) {
	if !value.IsValid() {
		return
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			walkScriptAST(value.Elem(), visited, visit)
		}
	case reflect.Pointer:
		if value.IsNil() {
			return
		}
		pointer := astPointer{typeOf: value.Type(), value: value.Pointer()}
		if _, found := visited[pointer]; found {
			return
		}
		visited[pointer] = struct{}{}
		if value.CanInterface() {
			visit(value.Interface())
		}
		walkScriptAST(value.Elem(), visited, visit)
	case reflect.Struct:
		if value.Type().PkgPath() != scriptASTPackage {
			return
		}
		for index := 0; index < value.NumField(); index++ {
			field := value.Type().Field(index)
			if field.PkgPath == "" {
				walkScriptAST(value.Field(index), visited, visit)
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			walkScriptAST(value.Index(index), visited, visit)
		}
	}
}
