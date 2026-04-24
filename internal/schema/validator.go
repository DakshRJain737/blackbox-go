// Package schema validates incoming messages against a JSON-defined schema.
// Each topic can define required fields and their expected types.
package schema
 
import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)
 
// FieldDef defines one expected field in a topic message.
type FieldDef struct {
	Type     string `json:"type"`     // "number" | "string" | "boolean"
	Required bool   `json:"required"` // if true, absence is an error
}
 
// TopicSchema defines the expected shape of messages for one topic.
type TopicSchema struct {
	Fields map[string]FieldDef `json:"fields"`
}
 
// Validator holds schemas for all known topics.
type Validator struct {
	mu      sync.RWMutex
	schemas map[string]TopicSchema // topic → schema
}
 
// New loads schemas from a JSON file.
// If the file doesn't exist, the validator is permissive (allows anything).
func New(path string) *Validator {
	v := &Validator{
		schemas: make(map[string]TopicSchema),
	}
	if err := v.Load(path); err != nil {
		log.Printf("⚠️  Schema file not loaded (%v) — running in permissive mode", err)
	}
	return v
}
 
// Load reads schema definitions from a JSON file.
func (v *Validator) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open schema file: %w", err)
	}
	defer f.Close()
 
	var schemas map[string]TopicSchema
	if err := json.NewDecoder(f).Decode(&schemas); err != nil {
		return fmt.Errorf("decode schema: %w", err)
	}
 
	v.mu.Lock()
	v.schemas = schemas
	v.mu.Unlock()
 
	log.Printf("✅ Loaded schemas for %d topics", len(schemas))
	return nil
}
 
// Validate checks a raw message payload against the schema for its topic.
// Returns nil if the message is valid or if no schema is defined for the topic.
func (v *Validator) Validate(topic string, payload map[string]any) error {
	v.mu.RLock()
	schema, ok := v.schemas[topic]
	v.mu.RUnlock()
 
	if !ok {
		// No schema for this topic — permissive pass
		return nil
	}
 
	for fieldName, def := range schema.Fields {
		val, exists := payload[fieldName]
		if !exists {
			if def.Required {
				return fmt.Errorf("missing required field '%s'", fieldName)
			}
			continue
		}
 
		if err := checkType(fieldName, val, def.Type); err != nil {
			return err
		}
	}
 
	return nil
}
 
func checkType(field string, val any, expected string) error {
	switch expected {
	case "number":
		switch val.(type) {
		case float64, float32, int, int64, json.Number:
			return nil
		}
		return fmt.Errorf("field '%s' expected number, got %T", field, val)
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("field '%s' expected string, got %T", field, val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field '%s' expected boolean, got %T", field, val)
		}
	}
	return nil
}
 