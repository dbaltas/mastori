package datasets

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Selection struct {
	Start int `yaml:"start"`
	Count int `yaml:"count"`
}

type SpotCheck struct {
	Index    int    `yaml:"index"`
	Contains string `yaml:"contains"`
}

type ValidateRules struct {
	MinRows        int         `yaml:"min_rows"`
	RequiredFields []string    `yaml:"required_fields"`
	SpotChecks     []SpotCheck `yaml:"spot_checks"`
}

type Dataset struct {
	Name        string        `yaml:"-"`
	Description string        `yaml:"description"`
	Source      string        `yaml:"source"`
	Config      string        `yaml:"config"`
	Split       string        `yaml:"split"`
	Revision    string        `yaml:"revision"`
	Extract     string        `yaml:"extract"`
	Selection   Selection     `yaml:"selection"`
	Validate    ValidateRules `yaml:"validate"`
}

type Manifest struct {
	Datasets map[string]Dataset `yaml:"datasets"`
}

func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	if len(m.Datasets) == 0 {
		return nil, fmt.Errorf("manifest %s contains no datasets", path)
	}
	for name, d := range m.Datasets {
		d.Name = name
		m.Datasets[name] = d
	}
	return &m, nil
}

func (m *Manifest) Get(name string) (Dataset, error) {
	d, ok := m.Datasets[name]
	if !ok {
		return Dataset{}, fmt.Errorf("dataset %q not found in manifest (available: %d entries)", name, len(m.Datasets))
	}
	return d, nil
}
