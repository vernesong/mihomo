package modules

import (
	"bytes"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func parseDocument(buf []byte) (*yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(buf))
	document := &yaml.Node{}
	if err := decoder.Decode(document); err != nil {
		return nil, err
	}

	return document, nil
}

func documentRoot(document *yaml.Node) *yaml.Node {
	if document == nil || document.Kind != yaml.DocumentNode || len(document.Content) == 0 {
		return nil
	}
	return document.Content[0]
}

func validateOverride(buf []byte) error {
	document, err := parseDocument(buf)
	if err != nil {
		return err
	}
	root := documentRoot(document)
	if root == nil || root.Kind != yaml.MappingNode {
		return fmt.Errorf("module content must be a YAML mapping")
	}
	return nil
}

type namedOverride struct {
	name    string
	content []byte
}

func mergeConfig(base []byte, overrides []namedOverride) ([]byte, error) {
	if len(overrides) == 0 {
		return base, nil
	}

	document, err := parseDocument(base)
	if err != nil {
		return nil, err
	}
	target := documentRoot(document)
	if target == nil || target.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("configuration must be a YAML mapping when modules are enabled")
	}

	for _, override := range overrides {
		overrideDocument, err := parseDocument(override.content)
		if err != nil {
			return nil, fmt.Errorf("module %q: %w", override.name, err)
		}
		source := documentRoot(overrideDocument)
		if source == nil || source.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("module %q: content must be a YAML mapping", override.name)
		}
		if err := mergeMapping(target, source); err != nil {
			return nil, fmt.Errorf("module %q: %w", override.name, err)
		}
	}

	return yaml.Marshal(document)
}

func mergeMapping(target, source *yaml.Node) error {
	for index := 0; index < len(source.Content); index += 2 {
		sourceKey := source.Content[index]
		sourceValue := source.Content[index+1]
		key, err := mappingKey(sourceKey)
		if err != nil {
			return err
		}

		switch sourceValue.Kind {
		case yaml.MappingNode:
			if strings.HasSuffix(key, "!") {
				logicalKey := trimWrappedKey(strings.TrimSuffix(key, "!"))
				setMappingValue(target, sourceKey, logicalKey, sourceValue)
				continue
			}

			logicalKey := trimWrappedKey(key)
			targetValue, found := getMappingValue(target, logicalKey)
			if !found || targetValue.Kind != yaml.MappingNode {
				targetValue = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
				setMappingValue(target, sourceKey, logicalKey, targetValue)
			}
			if err := mergeMapping(targetValue, sourceValue); err != nil {
				return fmt.Errorf("merge %q: %w", logicalKey, err)
			}

		case yaml.SequenceNode:
			logicalKey := trimWrappedKey(key)
			prepend := strings.HasPrefix(key, "+")
			appendValues := !prepend && strings.HasSuffix(key, "+")
			if prepend {
				logicalKey = trimWrappedKey(strings.TrimPrefix(key, "+"))
			} else if appendValues {
				logicalKey = trimWrappedKey(strings.TrimSuffix(key, "+"))
			}

			if !prepend && !appendValues {
				setMappingValue(target, sourceKey, logicalKey, sourceValue)
				continue
			}

			targetValue, found := getMappingValue(target, logicalKey)
			if !found {
				targetValue = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
				setMappingValue(target, sourceKey, logicalKey, targetValue)
			}
			if targetValue.Kind != yaml.SequenceNode {
				return fmt.Errorf("cannot merge array %q into non-array value", logicalKey)
			}
			if prepend {
				targetValue.Content = append(append([]*yaml.Node{}, sourceValue.Content...), targetValue.Content...)
			} else {
				targetValue.Content = append(targetValue.Content, sourceValue.Content...)
			}

		default:
			// Scalar keys are intentionally not unwrapped. The <key> escape is
			// only meaningful when a mapping or array key could be interpreted as
			// an override operator.
			setMappingValue(target, sourceKey, key, sourceValue)
		}
	}
	return nil
}

func mappingKey(node *yaml.Node) (string, error) {
	var key string
	if err := node.Decode(&key); err != nil {
		return "", fmt.Errorf("module mapping key must be a string: %w", err)
	}
	return key, nil
}

func trimWrappedKey(key string) string {
	if strings.HasPrefix(key, "<") && strings.HasSuffix(key, ">") {
		return strings.TrimSuffix(strings.TrimPrefix(key, "<"), ">")
	}
	return key
}

func getMappingValue(mapping *yaml.Node, key string) (*yaml.Node, bool) {
	for index := 0; index < len(mapping.Content); index += 2 {
		candidate, err := mappingKey(mapping.Content[index])
		if err == nil && candidate == key {
			return mapping.Content[index+1], true
		}
	}
	return nil, false
}

func setMappingValue(mapping, sourceKey *yaml.Node, key string, value *yaml.Node) {
	for index := 0; index < len(mapping.Content); index += 2 {
		candidate, err := mappingKey(mapping.Content[index])
		if err == nil && candidate == key {
			mapping.Content[index+1] = value
			return
		}
	}

	keyNode := *sourceKey
	keyNode.Value = key
	mapping.Content = append(mapping.Content, &keyNode, value)
}
