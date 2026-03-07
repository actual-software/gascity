package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// AcceptStartupDialogs dismisses Claude Code startup dialogs that can block
// automated sessions. Handles (in order):
//  1. Workspace trust dialog ("Quick safety check" / "trust this folder")
//  2. Bypass permissions warning ("Bypass Permissions mode") — requires Down+Enter
//
// The peek function should return the last N lines of the session's terminal output.
// The sendKeys function should send bare tmux-style keystrokes (e.g., "Enter", "Down").
//
// Idempotent: safe to call on sessions without dialogs.
func AcceptStartupDialogs(
	ctx context.Context,
	peek func(lines int) (string, error),
	sendKeys func(keys ...string) error,
) error {
	if err := acceptWorkspaceTrustDialog(ctx, peek, sendKeys); err != nil {
		return fmt.Errorf("workspace trust dialog: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := acceptBypassPermissionsWarning(ctx, peek, sendKeys); err != nil {
		return fmt.Errorf("bypass permissions warning: %w", err)
	}
	return nil
}

// acceptWorkspaceTrustDialog dismisses the Claude Code workspace trust dialog.
// Starting with Claude Code v2.1.55, a "Quick safety check" dialog appears on
// first launch in a workspace. Option 1 ("Yes, I trust this folder") is
// pre-selected, so pressing Enter accepts.
func acceptWorkspaceTrustDialog(
	ctx context.Context,
	peek func(lines int) (string, error),
	sendKeys func(keys ...string) error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
	}

	content, err := peek(30)
	if err != nil {
		return err
	}

	if !strings.Contains(content, "trust this folder") && !strings.Contains(content, "Quick safety check") {
		return nil
	}

	if err := sendKeys("Enter"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}
	return nil
}

// acceptBypassPermissionsWarning dismisses the Claude Code bypass permissions
// warning. When Claude starts with --dangerously-skip-permissions, it shows a
// warning requiring Down arrow to select "Yes, I accept" and then Enter.
func acceptBypassPermissionsWarning(
	ctx context.Context,
	peek func(lines int) (string, error),
	sendKeys func(keys ...string) error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
	}

	content, err := peek(30)
	if err != nil {
		return err
	}

	if !strings.Contains(content, "Bypass Permissions mode") {
		return nil
	}

	if err := sendKeys("Down"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}

	return sendKeys("Enter")
}
