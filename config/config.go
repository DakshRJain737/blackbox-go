package main
 
import (
	"encoding/json"
	"fmt"
	"os"
)
 
// Config is the top-level broker configuration.
type Config struct {
	HTTPAddr       string         `json:"http_addr"`
	DBPath         string         `json:"db_path"`
	SchemaPath     string         `json:"schema_path"`
	ThresholdsPath string         `json:"thresholds_path"`
	Devices        []DeviceConfig `json:"devices"`
}
 
// DeviceConfig represents one serial-connected device (e.g. an Arduino).
type DeviceConfig struct {
	Name     string `json:"name"`
	Port     string `json:"port"`
	BaudRate int    `json:"baud_rate"`
}
 
func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer f.Close()
 
	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
 
	// Sensible defaults
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "blackbox.db"
	}
	if cfg.SchemaPath == "" {
		cfg.SchemaPath = "config/schema.json"
	}
	if cfg.ThresholdsPath == "" {
		cfg.ThresholdsPath = "config/thresholds.json"
	}
 
	return &cfg, nil
}