package config

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var validServiceName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// Service declares a workspace-owned HTTP service mounted under /svc/{name}.
type Service struct {
	// Name is the unique service identifier within a workspace.
	Name string `toml:"name" jsonschema:"required"`
	// Kind selects how the service is implemented.
	Kind string `toml:"kind,omitempty" jsonschema:"enum=workflow,enum=proxy_process"`
	// MountPath is the HTTP prefix served by the controller. Defaults to /svc/{name}.
	MountPath string `toml:"mount_path,omitempty"`
	// PublishMode declares how the service is intended to be published.
	// v0 supports local/private and direct reuse of the API listener; relay
	// publication is a future hosted slice.
	PublishMode string `toml:"publish_mode,omitempty" jsonschema:"enum=private,enum=direct,enum=relay"`
	// Audience describes the intended caller class.
	Audience string `toml:"audience,omitempty" jsonschema:"enum=operator,enum=tenant,enum=public"`
	// AuthMode selects the expected caller proof model.
	AuthMode string `toml:"auth_mode,omitempty" jsonschema:"enum=none,enum=bearer,enum=shared_secret,enum=github_hmac,enum=delegated"`
	// DesiredHostname is reserved for future relay/direct publication backends.
	DesiredHostname string `toml:"desired_hostname,omitempty"`
	// HealthPath is an optional service-local health endpoint path.
	HealthPath string `toml:"health_path,omitempty"`
	// StateRoot overrides the managed service state root. Defaults to
	// .gc/services/{name}. The path must stay within .gc/services/.
	StateRoot string `toml:"state_root,omitempty"`
	// Workflow configures controller-owned workflow services.
	Workflow ServiceWorkflowConfig `toml:"workflow,omitempty"`
	// ProxyProcess configures reverse-proxied child processes.
	ProxyProcess ServiceProxyProcessConfig `toml:"proxy_process,omitempty"`
	// SourceDir records pack provenance for pack-stamped services.
	SourceDir string `toml:"-" json:"-"`
}

// KindOrDefault returns the normalized service kind.
func (s Service) KindOrDefault() string {
	if s.Kind == "" {
		return "workflow"
	}
	return s.Kind
}

// MountPathOrDefault returns the service mount path.
func (s Service) MountPathOrDefault() string {
	if s.MountPath != "" {
		return s.MountPath
	}
	return "/svc/" + s.Name
}

// PublishModeOrDefault returns the normalized publish mode.
func (s Service) PublishModeOrDefault() string {
	if s.PublishMode == "" {
		return "private"
	}
	return s.PublishMode
}

// AudienceOrDefault returns the normalized audience.
func (s Service) AudienceOrDefault() string {
	if s.Audience == "" {
		return "operator"
	}
	return s.Audience
}

// AuthModeOrDefault returns the normalized auth mode.
func (s Service) AuthModeOrDefault() string {
	if s.AuthMode == "" {
		if s.PublishModeOrDefault() == "private" {
			return "none"
		}
		return "shared_secret"
	}
	return s.AuthMode
}

// StateRootOrDefault returns the managed runtime root for the service.
func (s Service) StateRootOrDefault() string {
	if s.StateRoot != "" {
		return filepath.Clean(s.StateRoot)
	}
	return filepath.Join(".gc", "services", s.Name)
}

// ServiceWorkflowConfig configures controller-owned workflow services.
type ServiceWorkflowConfig struct {
	// Contract selects the built-in workflow handler.
	Contract string `toml:"contract,omitempty"`
}

// ServiceProxyProcessConfig configures a reverse-proxied child process.
type ServiceProxyProcessConfig struct {
	// Command is the executable to launch.
	Command string `toml:"command,omitempty"`
	// Args are appended to Command.
	Args []string `toml:"args,omitempty"`
	// HealthPath is the child-local health endpoint.
	HealthPath string `toml:"health_path,omitempty"`
}

// ValidateServices checks workspace service declarations for configuration
// errors that would prevent runtime activation.
func ValidateServices(services []Service) error {
	seen := make(map[string]bool, len(services))
	for i, svc := range services {
		if svc.Name == "" {
			return fmt.Errorf("service[%d]: name is required", i)
		}
		if !validServiceName.MatchString(svc.Name) {
			return fmt.Errorf("service %q: name must match [a-zA-Z0-9][a-zA-Z0-9_-]*", svc.Name)
		}
		if seen[svc.Name] {
			if svc.SourceDir != "" {
				return fmt.Errorf("service %q: duplicate name (from %q)", svc.Name, svc.SourceDir)
			}
			return fmt.Errorf("service %q: duplicate name", svc.Name)
		}
		seen[svc.Name] = true

		switch svc.KindOrDefault() {
		case "workflow", "proxy_process":
		default:
			return fmt.Errorf("service %q: kind must be \"workflow\" or \"proxy_process\", got %q", svc.Name, svc.Kind)
		}
		switch svc.PublishModeOrDefault() {
		case "private", "direct", "relay":
		default:
			return fmt.Errorf("service %q: publish_mode must be \"private\", \"direct\", or \"relay\", got %q", svc.Name, svc.PublishMode)
		}
		switch svc.AudienceOrDefault() {
		case "operator", "tenant", "public":
		default:
			return fmt.Errorf("service %q: audience must be \"operator\", \"tenant\", or \"public\", got %q", svc.Name, svc.Audience)
		}
		switch svc.AuthModeOrDefault() {
		case "none", "bearer", "shared_secret", "github_hmac", "delegated":
		default:
			return fmt.Errorf("service %q: auth_mode must be one of none, bearer, shared_secret, github_hmac, delegated, got %q", svc.Name, svc.AuthMode)
		}

		wantMount := "/svc/" + svc.Name
		if got := filepath.ToSlash(filepath.Clean(svc.MountPathOrDefault())); got != wantMount {
			return fmt.Errorf("service %q: mount_path must be %q in v0, got %q", svc.Name, wantMount, svc.MountPathOrDefault())
		}
		root := filepath.ToSlash(filepath.Clean(svc.StateRootOrDefault()))
		if !strings.HasPrefix(root, ".gc/services/") {
			return fmt.Errorf("service %q: state_root must stay under .gc/services/, got %q", svc.Name, svc.StateRootOrDefault())
		}
		if strings.Contains(root, "../") || strings.HasSuffix(root, "/..") {
			return fmt.Errorf("service %q: state_root may not traverse upward, got %q", svc.Name, svc.StateRootOrDefault())
		}

		switch svc.KindOrDefault() {
		case "workflow":
			if svc.Workflow.Contract == "" {
				return fmt.Errorf("service %q: workflow.contract is required for workflow services", svc.Name)
			}
			if svc.AuthModeOrDefault() == "delegated" {
				return fmt.Errorf("service %q: auth_mode=delegated is invalid for workflow services", svc.Name)
			}
		case "proxy_process":
			if svc.ProxyProcess.Command == "" {
				return fmt.Errorf("service %q: proxy_process.command is required for proxy_process services", svc.Name)
			}
		}
	}
	return nil
}
