package script

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/grafana/sobek"
)

type scriptURL struct {
	vm     *sobek.Runtime
	parsed *url.URL
}

func (h *runtimeHost) installURLGlobal() error {
	constructor := h.vm.ToValue(func(call sobek.ConstructorCall) *sobek.Object {
		if len(call.Arguments) == 0 {
			panic(h.vm.NewTypeError("URL requires an input"))
		}
		input := call.Argument(0).String()
		base := ""
		if len(call.Arguments) > 1 && !sobek.IsUndefined(call.Argument(1)) {
			base = call.Argument(1).String()
		}
		parsed, err := parseScriptURL(input, base)
		if err != nil {
			panic(h.vm.NewTypeError("invalid URL: %s", err.Error()))
		}
		state := &scriptURL{vm: h.vm, parsed: parsed}
		if err = state.install(call.This); err != nil {
			panic(err)
		}
		return nil
	})
	if err := h.vm.Set("URL", constructor); err != nil {
		return err
	}
	object := constructor.ToObject(h.vm)
	_ = object.Set("canParse", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			return h.vm.ToValue(false)
		}
		base := ""
		if len(call.Arguments) > 1 && !sobek.IsUndefined(call.Argument(1)) {
			base = call.Argument(1).String()
		}
		_, err := parseScriptURL(call.Argument(0).String(), base)
		return h.vm.ToValue(err == nil)
	})
	return nil
}

func parseScriptURL(input, base string) (*url.URL, error) {
	reference, err := url.Parse(input)
	if err != nil {
		return nil, err
	}
	if base != "" {
		baseURL, baseErr := url.Parse(base)
		if baseErr != nil || !baseURL.IsAbs() {
			if baseErr != nil {
				return nil, baseErr
			}
			return nil, fmt.Errorf("base is not absolute")
		}
		reference = baseURL.ResolveReference(reference)
	}
	if !reference.IsAbs() {
		return nil, fmt.Errorf("URL is not absolute")
	}
	if (reference.Scheme == "http" || reference.Scheme == "https") && reference.Host == "" {
		return nil, fmt.Errorf("URL has no host")
	}
	return reference, nil
}

func (u *scriptURL) install(object *sobek.Object) error {
	readonly := map[string]func() string{
		"origin": func() string {
			if u.parsed.Scheme == "" || u.parsed.Host == "" {
				return "null"
			}
			return u.parsed.Scheme + "://" + u.parsed.Host
		},
	}
	for name, getter := range readonly {
		if err := u.defineAccessor(object, name, getter, nil); err != nil {
			return err
		}
	}
	accessors := []struct {
		name string
		get  func() string
		set  func(string) error
	}{
		{name: "href", get: u.href, set: u.setHref},
		{name: "protocol", get: func() string { return u.parsed.Scheme + ":" }, set: u.setProtocol},
		{name: "username", get: u.username, set: u.setUsername},
		{name: "password", get: u.password, set: u.setPassword},
		{name: "host", get: func() string { return u.parsed.Host }, set: u.setHost},
		{name: "hostname", get: u.parsed.Hostname, set: u.setHostname},
		{name: "port", get: u.parsed.Port, set: u.setPort},
		{name: "pathname", get: u.pathname, set: u.setPathname},
		{name: "search", get: u.search, set: u.setSearch},
		{name: "hash", get: u.hash, set: u.setHash},
	}
	for _, accessor := range accessors {
		if err := u.defineAccessor(object, accessor.name, accessor.get, accessor.set); err != nil {
			return err
		}
	}
	searchParams := u.searchParamsObject()
	if err := object.DefineDataProperty("searchParams", searchParams, sobek.FLAG_FALSE, sobek.FLAG_TRUE, sobek.FLAG_FALSE); err != nil {
		return err
	}
	stringValue := func(sobek.FunctionCall) sobek.Value { return u.vm.ToValue(u.href()) }
	if err := object.DefineDataProperty("toString", u.vm.ToValue(stringValue), sobek.FLAG_TRUE, sobek.FLAG_TRUE, sobek.FLAG_FALSE); err != nil {
		return err
	}
	return object.DefineDataProperty("toJSON", u.vm.ToValue(stringValue), sobek.FLAG_TRUE, sobek.FLAG_TRUE, sobek.FLAG_FALSE)
}

func (u *scriptURL) defineAccessor(object *sobek.Object, name string, get func() string, set func(string) error) error {
	getter := u.vm.ToValue(func(sobek.FunctionCall) sobek.Value {
		return u.vm.ToValue(get())
	})
	var setter sobek.Value
	if set != nil {
		setter = u.vm.ToValue(func(call sobek.FunctionCall) sobek.Value {
			if err := set(call.Argument(0).String()); err != nil {
				panic(u.vm.NewTypeError("invalid URL %s: %s", name, err.Error()))
			}
			return sobek.Undefined()
		})
	}
	return object.DefineAccessorProperty(name, getter, setter, sobek.FLAG_TRUE, sobek.FLAG_FALSE)
}

func (u *scriptURL) href() string {
	return u.parsed.String()
}

func (u *scriptURL) username() string {
	if u.parsed.User == nil {
		return ""
	}
	return u.parsed.User.Username()
}

func (u *scriptURL) password() string {
	if u.parsed.User == nil {
		return ""
	}
	password, _ := u.parsed.User.Password()
	return password
}

func (u *scriptURL) pathname() string {
	path := u.parsed.EscapedPath()
	if path == "" && u.parsed.Host != "" {
		return "/"
	}
	return path
}

func (u *scriptURL) search() string {
	if u.parsed.RawQuery == "" && !u.parsed.ForceQuery {
		return ""
	}
	return "?" + u.parsed.RawQuery
}

func (u *scriptURL) hash() string {
	if u.parsed.Fragment == "" {
		return ""
	}
	return "#" + u.parsed.EscapedFragment()
}

func (u *scriptURL) setHref(value string) error {
	parsed, err := parseScriptURL(value, "")
	if err == nil {
		u.parsed = parsed
	}
	return err
}

func (u *scriptURL) setProtocol(value string) error {
	value = strings.TrimSuffix(value, ":")
	if value == "" {
		return fmt.Errorf("protocol is empty")
	}
	u.parsed.Scheme = strings.ToLower(value)
	return nil
}

func (u *scriptURL) setUsername(value string) error {
	password := u.password()
	if password == "" {
		u.parsed.User = url.User(value)
	} else {
		u.parsed.User = url.UserPassword(value, password)
	}
	return nil
}

func (u *scriptURL) setPassword(value string) error {
	u.parsed.User = url.UserPassword(u.username(), value)
	return nil
}

func (u *scriptURL) setHost(value string) error {
	if _, err := url.Parse("//" + value); err != nil {
		return err
	}
	u.parsed.Host = value
	return nil
}

func (u *scriptURL) setHostname(value string) error {
	port := u.parsed.Port()
	if port == "" {
		u.parsed.Host = value
	} else {
		u.parsed.Host = net.JoinHostPort(strings.Trim(value, "[]"), port)
	}
	return nil
}

func (u *scriptURL) setPort(value string) error {
	hostname := u.parsed.Hostname()
	if value == "" {
		u.parsed.Host = hostname
		return nil
	}
	if _, err := url.Parse("http://" + net.JoinHostPort(hostname, value)); err != nil {
		return err
	}
	u.parsed.Host = net.JoinHostPort(hostname, value)
	return nil
}

func (u *scriptURL) setPathname(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return err
	}
	u.parsed.Path = parsed.Path
	u.parsed.RawPath = parsed.RawPath
	return nil
}

func (u *scriptURL) setSearch(value string) error {
	forceQuery := value == "?"
	value = strings.TrimPrefix(value, "?")
	if _, err := url.ParseQuery(value); err != nil {
		return err
	}
	u.parsed.RawQuery = value
	u.parsed.ForceQuery = forceQuery
	return nil
}

func (u *scriptURL) setHash(value string) error {
	value = strings.TrimPrefix(value, "#")
	fragment, err := url.PathUnescape(value)
	if err != nil {
		return err
	}
	u.parsed.Fragment = fragment
	u.parsed.RawFragment = value
	return nil
}

func (u *scriptURL) searchParamsObject() *sobek.Object {
	object := u.vm.NewObject()
	method := func(name string, function func(sobek.FunctionCall) sobek.Value) {
		_ = object.DefineDataProperty(name, u.vm.ToValue(function), sobek.FLAG_TRUE, sobek.FLAG_TRUE, sobek.FLAG_FALSE)
	}
	method("append", func(call sobek.FunctionCall) sobek.Value {
		values := u.query()
		values.Add(call.Argument(0).String(), call.Argument(1).String())
		u.storeQuery(values)
		return sobek.Undefined()
	})
	method("delete", func(call sobek.FunctionCall) sobek.Value {
		values := u.query()
		values.Del(call.Argument(0).String())
		u.storeQuery(values)
		return sobek.Undefined()
	})
	method("get", func(call sobek.FunctionCall) sobek.Value {
		values, found := u.query()[call.Argument(0).String()]
		if !found || len(values) == 0 {
			return sobek.Null()
		}
		return u.vm.ToValue(values[0])
	})
	method("getAll", func(call sobek.FunctionCall) sobek.Value {
		return u.vm.ToValue(append([]string(nil), u.query()[call.Argument(0).String()]...))
	})
	method("has", func(call sobek.FunctionCall) sobek.Value {
		_, found := u.query()[call.Argument(0).String()]
		return u.vm.ToValue(found)
	})
	method("set", func(call sobek.FunctionCall) sobek.Value {
		values := u.query()
		values.Set(call.Argument(0).String(), call.Argument(1).String())
		u.storeQuery(values)
		return sobek.Undefined()
	})
	method("sort", func(sobek.FunctionCall) sobek.Value {
		u.storeQuery(u.query())
		return sobek.Undefined()
	})
	method("toString", func(sobek.FunctionCall) sobek.Value {
		return u.vm.ToValue(u.parsed.RawQuery)
	})
	return object
}

func (u *scriptURL) query() url.Values {
	values, _ := url.ParseQuery(u.parsed.RawQuery)
	return values
}

func (u *scriptURL) storeQuery(values url.Values) {
	u.parsed.RawQuery = values.Encode()
	u.parsed.ForceQuery = false
}
