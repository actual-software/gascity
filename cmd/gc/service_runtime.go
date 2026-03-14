package main

import (
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/workspacesvc"
)

type serviceRuntime struct {
	cr *CityRuntime
}

var _ workspacesvc.RuntimeContext = (*serviceRuntime)(nil)

func (rt *serviceRuntime) CityPath() string {
	return rt.cr.cityPath
}

func (rt *serviceRuntime) CityName() string {
	return rt.cr.cityName
}

func (rt *serviceRuntime) Config() *config.City {
	return rt.cr.cfg
}

func (rt *serviceRuntime) SessionProvider() runtime.Provider {
	return rt.cr.sp
}

func (rt *serviceRuntime) BeadStore(rig string) beads.Store {
	if rt.cr.cs != nil {
		return rt.cr.cs.BeadStore(rig)
	}
	for _, candidate := range rt.cr.cfg.Rigs {
		if candidate.Name == rig {
			return beads.NewBdStore(candidate.Path, beads.ExecCommandRunner())
		}
	}
	return nil
}

func (rt *serviceRuntime) Poke() {
	select {
	case rt.cr.pokeCh <- struct{}{}:
	default:
	}
}
