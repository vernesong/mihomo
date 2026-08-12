package modules

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/metacubex/mihomo/component/age"

	"gopkg.in/yaml.v3"
)

func SetEnabled(source []byte, name string, enabled bool) ([]byte, bool, error) {
	if strings.HasPrefix(string(source), age.FileHeader) {
		return nil, false, fmt.Errorf("cannot update modules in an encrypted configuration file")
	}

	document, err := parseDocument(source)
	if err != nil {
		return nil, false, err
	}
	root := documentRoot(document)
	if root == nil || root.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("configuration must be a YAML mapping")
	}
	modulesNode, found := getMappingValue(root, "modules")
	if !found || modulesNode.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	moduleNode, found := getMappingValue(modulesNode, name)
	if !found {
		return nil, false, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if moduleNode.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("module %q must be a YAML mapping", name)
	}
	enableNode, found := getMappingValue(moduleNode, "enable")
	if !found {
		return nil, false, fmt.Errorf("module %q: enable is required", name)
	}
	var current bool
	if err := enableNode.Decode(&current); err != nil {
		return nil, false, fmt.Errorf("module %q: enable must be a boolean", name)
	}
	if current == enabled {
		return source, false, nil
	}

	enableNode.Kind = yaml.ScalarNode
	enableNode.Tag = "!!bool"
	enableNode.Value = strconv.FormatBool(enabled)
	enableNode.Style = 0
	updated, err := yaml.Marshal(document)
	if err != nil {
		return nil, false, err
	}
	return updated, true, nil
}

func SetOrder(source []byte, order []string) ([]byte, bool, error) {
	if strings.HasPrefix(string(source), age.FileHeader) {
		return nil, false, fmt.Errorf("cannot update modules in an encrypted configuration file")
	}

	document, err := parseDocument(source)
	if err != nil {
		return nil, false, err
	}
	root := documentRoot(document)
	if root == nil || root.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("configuration must be a YAML mapping")
	}
	modulesNode, found := getMappingValue(root, "modules")
	if !found {
		if len(order) == 0 {
			return source, false, nil
		}
		return nil, false, fmt.Errorf("modules are not configured")
	}
	if modulesNode.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("modules must be a YAML mapping")
	}

	indices := make(map[string]int, len(modulesNode.Content)/2)
	current := make([]string, 0, len(modulesNode.Content)/2)
	for index := 0; index < len(modulesNode.Content); index += 2 {
		name, err := mappingKey(modulesNode.Content[index])
		if err != nil {
			return nil, false, fmt.Errorf("modules: %w", err)
		}
		if _, exists := indices[name]; exists {
			return nil, false, fmt.Errorf("module name %q is duplicated", name)
		}
		indices[name] = index
		current = append(current, name)
	}

	seen := make(map[string]struct{}, len(order))
	for _, name := range order {
		if _, exists := seen[name]; exists {
			return nil, false, fmt.Errorf("module order contains duplicate module %q", name)
		}
		seen[name] = struct{}{}
		if _, exists := indices[name]; !exists {
			return nil, false, fmt.Errorf("module order contains unknown module %q", name)
		}
	}

	missing := make([]string, 0, len(indices)-len(seen))
	for _, name := range current {
		if _, exists := seen[name]; !exists {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		return nil, false, fmt.Errorf("module order is missing modules: %s", strings.Join(missing, ", "))
	}

	unchanged := len(current) == len(order)
	for index := range current {
		if current[index] != order[index] {
			unchanged = false
			break
		}
	}
	if unchanged {
		return source, false, nil
	}

	reordered := make([]*yaml.Node, 0, len(modulesNode.Content))
	for _, name := range order {
		index := indices[name]
		reordered = append(reordered, modulesNode.Content[index], modulesNode.Content[index+1])
	}
	modulesNode.Content = reordered
	updated, err := yaml.Marshal(document)
	if err != nil {
		return nil, false, err
	}
	return updated, true, nil
}
