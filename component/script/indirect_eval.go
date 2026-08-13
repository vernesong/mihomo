package script

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/grafana/sobek"
	jsAST "github.com/grafana/sobek/ast"
	"github.com/grafana/sobek/file"
	"github.com/grafana/sobek/unistring"
)

type sourceSpan struct {
	start int
	end   int
}

func (e *entrySpec) compileProgram(content []byte) (*sobek.Program, error) {
	source := string(content)
	programAST, err := sobek.Parse(e.path, source)
	if err != nil {
		return nil, fmt.Errorf("compile JavaScript: %w", err)
	}
	if e.indirectEval {
		var changed bool
		source, changed, err = rewriteDirectEvalCalls(source, programAST)
		if err != nil {
			return nil, fmt.Errorf("compile JavaScript: %w", err)
		}
		if changed {
			programAST, err = sobek.Parse(e.path, source)
			if err != nil {
				return nil, fmt.Errorf("compile JavaScript: %w", err)
			}
		}
	}
	rewriteClassFieldArrows(programAST, source)
	program, err := sobek.CompileAST(programAST, false)
	if err != nil {
		return nil, fmt.Errorf("compile JavaScript: %w", err)
	}
	return program, nil
}

func rewriteDirectEvalCalls(source string, program *jsAST.Program) (string, bool, error) {
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
			return "", false, fmt.Errorf("invalid direct eval source span [%d:%d]", start, end)
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
	return source, len(spans) != 0, nil
}

// Sobek currently carries the caller's argument count into a nested class
// field initializer. An arrow field that captures this then computes the wrong
// stack base when an instance is created inside a function with arguments.
// Create that arrow through a zero-argument function while preserving its
// lexical this binding. This is equivalent for field arrows that do not use
// super, and avoids changing the downloaded script on disk.
func rewriteClassFieldArrows(program *jsAST.Program, source string) {
	fields := make([]*jsAST.FieldDefinition, 0)
	walkScriptAST(reflect.ValueOf(program), make(map[astPointer]struct{}), func(value any) {
		field, ok := value.(*jsAST.FieldDefinition)
		if ok {
			fields = append(fields, field)
		}
	})
	for _, field := range fields {
		arrow, ok := field.Initializer.(*jsAST.ArrowFunctionLiteral)
		if !ok {
			continue
		}
		usesThis, usesSuper, usesArguments, usesNewTarget := classFieldArrowUsage(arrow)
		if !usesThis || usesSuper || usesArguments || usesNewTarget {
			continue
		}
		field.Initializer = classFieldArrowWrapper(arrow, source)
	}
}

func classFieldArrowUsage(arrow *jsAST.ArrowFunctionLiteral) (usesThis, usesSuper, usesArguments, usesNewTarget bool) {
	root := reflect.ValueOf(arrow)
	visited := make(map[astPointer]struct{})
	var inspect func(reflect.Value, bool)
	inspect = func(value reflect.Value, isRoot bool) {
		if !value.IsValid() || usesSuper || usesArguments || usesNewTarget {
			return
		}
		switch value.Kind() {
		case reflect.Interface:
			if !value.IsNil() {
				inspect(value.Elem(), false)
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
				switch node := value.Interface().(type) {
				case *jsAST.ThisExpression:
					usesThis = true
					return
				case *jsAST.SuperExpression:
					usesSuper = true
					return
				case *jsAST.Identifier:
					if node.Name.String() == "arguments" {
						usesArguments = true
						return
					}
				case *jsAST.MetaProperty:
					usesNewTarget = node.Meta != nil && node.Property != nil && node.Meta.Name.String() == "new" && node.Property.Name.String() == "target"
					if usesNewTarget {
						return
					}
				case *jsAST.FunctionLiteral, *jsAST.ClassLiteral:
					if !isRoot {
						return
					}
				}
			}
			inspect(value.Elem(), false)
		case reflect.Struct:
			if value.Type().PkgPath() != scriptASTPackage {
				return
			}
			for index := 0; index < value.NumField(); index++ {
				fieldValue := value.Type().Field(index)
				if fieldValue.PkgPath == "" {
					inspect(value.Field(index), false)
				}
			}
		case reflect.Slice, reflect.Array:
			for index := 0; index < value.Len(); index++ {
				inspect(value.Index(index), false)
			}
		}
	}
	inspect(root, true)
	return usesThis, usesSuper, usesArguments, usesNewTarget
}

func classFieldArrowWrapper(arrow *jsAST.ArrowFunctionLiteral, source string) jsAST.Expression {
	start := arrow.Idx0()
	end := arrow.Idx1()
	closing := end - 1
	arrowSource := ""
	startOffset, endOffset := int(start)-1, int(end)-1
	if startOffset >= 0 && endOffset >= startOffset && endOffset <= len(source) {
		arrowSource = source[startOffset:endOffset]
	}
	wrapper := &jsAST.FunctionLiteral{
		Function: start,
		ParameterList: &jsAST.ParameterList{
			Opening: start,
			Closing: start,
		},
		Body: &jsAST.BlockStatement{
			LeftBrace: start,
			List: []jsAST.Statement{&jsAST.ReturnStatement{
				Return:   start,
				Argument: arrow,
			}},
			RightBrace: closing,
		},
		Source: "function(){return (" + arrowSource + ")}",
	}
	return &jsAST.CallExpression{
		Callee: &jsAST.DotExpression{
			Left: wrapper,
			Identifier: jsAST.Identifier{
				Name: unistring.String("call"),
				Idx:  start,
			},
		},
		LeftParenthesis: start,
		ArgumentList: []jsAST.Expression{&jsAST.ThisExpression{
			Idx: start,
		}},
		RightParenthesis: file.Idx(closing),
	}
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
