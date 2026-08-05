package citylayout

import (
	"strings"
	"testing"
)

// TestPackRuntimeEnvExportsFactoryRoot pins the cause-side half of the
// dirty-cache loop. Pack scripts run with their working directory set to the
// pack dir, which for an imported pack lives inside gc's global import cache.
// A script that resolves its state dir against FACTORY_ROOT and falls back to
// the working directory therefore wrote into the cache clone and left it
// dirty, and the next gc invocation refused the dirty cache and dropped every
// pack check. Exporting FACTORY_ROOT keeps that fallback from ever firing.
func TestPackRuntimeEnvExportsFactoryRoot(t *testing.T) {
	const cityRoot = "/tmp/some-city"

	env := PackRuntimeEnv(cityRoot, "local-core")

	var got string
	var found bool
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok && k == "FACTORY_ROOT" {
			got, found = v, true
		}
	}
	if !found {
		t.Fatalf("FACTORY_ROOT not exported to pack scripts; env=%v", env)
	}
	if got != cityRoot {
		t.Errorf("FACTORY_ROOT = %q, want the city root %q", got, cityRoot)
	}
}

func TestPackRuntimeEnvMapExportsFactoryRoot(t *testing.T) {
	const cityRoot = "/tmp/some-city"

	env := PackRuntimeEnvMap(cityRoot, "local-core")

	if got := env["FACTORY_ROOT"]; got != cityRoot {
		t.Errorf("FACTORY_ROOT = %q, want the city root %q", got, cityRoot)
	}
}

// TestPackRuntimeEnvExportsFactoryRootWithoutPackName covers the unnamed-pack
// path: GC_PACK_STATE_DIR is conditional on the pack name, FACTORY_ROOT is not.
func TestPackRuntimeEnvExportsFactoryRootWithoutPackName(t *testing.T) {
	const cityRoot = "/tmp/some-city"

	for _, kv := range PackRuntimeEnv(cityRoot, "") {
		if kv == "FACTORY_ROOT="+cityRoot {
			return
		}
	}
	t.Errorf("FACTORY_ROOT not exported when packName is empty")
}
