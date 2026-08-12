package script

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/metacubex/mihomo/component/age"

	"gopkg.in/yaml.v3"
)

func SetEnabled(source []byte, name string, enabled bool) ([]byte, bool, error) {
	if strings.HasPrefix(string(source), age.FileHeader) {
		return nil, false, fmt.Errorf("cannot update scripts in an encrypted configuration file")
	}

	var document yaml.Node
	if err := yaml.Unmarshal(source, &document); err != nil {
		return nil, false, err
	}
	root := documentRoot(&document)
	if root == nil || root.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("configuration must be a YAML mapping")
	}
	scriptsNode, found := mappingValue(root, "scripts")
	if !found || scriptsNode.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	scriptNode, found := mappingValue(scriptsNode, name)
	if !found {
		return nil, false, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if scriptNode.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("script %q must be a YAML mapping", name)
	}
	enableNode, found := mappingValue(scriptNode, "enable")
	if !found {
		return nil, false, fmt.Errorf("script %q: enable is required", name)
	}
	var current bool
	if err := enableNode.Decode(&current); err != nil {
		return nil, false, fmt.Errorf("script %q: enable must be a boolean", name)
	}
	if current == enabled {
		return source, false, nil
	}

	enableNode.Kind = yaml.ScalarNode
	enableNode.Tag = "!!bool"
	enableNode.Value = strconv.FormatBool(enabled)
	enableNode.Style = 0
	updated, err := yaml.Marshal(&document)
	if err != nil {
		return nil, false, err
	}
	return updated, true, nil
}

func documentRoot(document *yaml.Node) *yaml.Node {
	if document == nil {
		return nil
	}
	if document.Kind == yaml.DocumentNode {
		if len(document.Content) == 0 {
			return nil
		}
		return document.Content[0]
	}
	return document
}

func mappingValue(mapping *yaml.Node, key string) (*yaml.Node, bool) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil, false
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1], true
		}
	}
	return nil, false
}
