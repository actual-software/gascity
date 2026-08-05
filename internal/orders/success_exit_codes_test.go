package orders

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseSuccessExitCodes(t *testing.T) {
	order, err := Parse([]byte(`[order]
exec = "scripts/branch_protection.py"
trigger = "cron"
schedule = "0 10 * * *"
success_exit_codes = [1]
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !reflect.DeepEqual(order.SuccessExitCodes, []int{1}) {
		t.Fatalf("SuccessExitCodes = %v, want [1]", order.SuccessExitCodes)
	}
}

func TestParseWithoutSuccessExitCodesLeavesItEmpty(t *testing.T) {
	order, err := Parse([]byte(`[order]
exec = "true"
trigger = "cooldown"
interval = "1h"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(order.SuccessExitCodes) != 0 {
		t.Fatalf("SuccessExitCodes = %v, want empty for an order that declares none", order.SuccessExitCodes)
	}
}

func TestIsSuccessExitCode(t *testing.T) {
	declared := Order{Name: "branch-protection", Exec: "scripts/branch_protection.py", SuccessExitCodes: []int{1}}
	plain := Order{Name: "preflight", Exec: "scripts/preflight.sh"}

	tests := []struct {
		name  string
		order Order
		code  int
		want  bool
	}{
		{name: "exit 0 always succeeds", order: plain, code: 0, want: true},
		{name: "exit 0 succeeds even with codes declared", order: declared, code: 0, want: true},
		{name: "declared informational code succeeds", order: declared, code: 1, want: true},
		{name: "undeclared code still fails", order: declared, code: 10, want: false},
		{name: "no declaration means any non-zero fails", order: plain, code: 1, want: false},
		// A signaled process reports -1. It must never be read as declared.
		{name: "signaled process is not a success", order: declared, code: -1, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.order.IsSuccessExitCode(tc.code); got != tc.want {
				t.Fatalf("IsSuccessExitCode(%d) = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
}

func TestValidateSuccessExitCodes(t *testing.T) {
	tests := []struct {
		name    string
		order   Order
		wantErr string
	}{
		{
			name:  "valid on an exec order",
			order: Order{Name: "branch-protection", Exec: "scripts/bp.py", Trigger: "cron", Schedule: "0 10 * * *", SuccessExitCodes: []int{1, 2}},
		},
		{
			name:    "rejected on a formula order",
			order:   Order{Name: "sweep", Formula: "sweep", Trigger: "cooldown", Interval: "1h", SuccessExitCodes: []int{1}},
			wantErr: "success_exit_codes is supported only for exec orders",
		},
		{
			name:    "zero is redundant and rejected",
			order:   Order{Name: "bp", Exec: "scripts/bp.py", Trigger: "cooldown", Interval: "1h", SuccessExitCodes: []int{0}},
			wantErr: "must not list 0",
		},
		{
			name:    "out-of-range code rejected",
			order:   Order{Name: "bp", Exec: "scripts/bp.py", Trigger: "cooldown", Interval: "1h", SuccessExitCodes: []int{256}},
			wantErr: "must be between 1 and 255",
		},
		{
			name:    "negative code rejected",
			order:   Order{Name: "bp", Exec: "scripts/bp.py", Trigger: "cooldown", Interval: "1h", SuccessExitCodes: []int{-1}},
			wantErr: "must be between 1 and 255",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.order)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}
