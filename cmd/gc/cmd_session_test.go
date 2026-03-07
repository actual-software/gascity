package main

import (
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "30s"},
		{5 * time.Minute, "5m"},
		{3 * time.Hour, "3h"},
		{48 * time.Hour, "2d"},
	}
	for _, tt := range tests {
		got := formatDuration(tt.d)
		if got != tt.want {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestParsePruneDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"7d", 7 * 24 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"24h", 24 * time.Hour, false},
		{"30m", 30 * time.Minute, false},
		{"-5d", 0, true},
		{"0d", 0, true},
		{"-24h", 0, true},
		{"0h", 0, true},
		{"1.5d", 0, true},
		{"7dd", 0, true},
		{"abc", 0, true},
		{"d", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parsePruneDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePruneDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parsePruneDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveWorkDir(t *testing.T) {
	tests := []struct {
		name     string
		cityPath string
		dir      string
		want     string
	}{
		{"city-scoped", "/home/user/city", "", "/home/user/city"},
		{"rig-scoped", "/home/user/city", "my-rig", "/home/user/city/rigs/my-rig"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &config.Agent{Dir: tt.dir}
			got := resolveWorkDir(tt.cityPath, a)
			if got != tt.want {
				t.Errorf("resolveWorkDir = %q, want %q", got, tt.want)
			}
		})
	}
}
