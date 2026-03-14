package workspacesvc

import "sync"

var (
	workflowFactoriesMu sync.RWMutex
	workflowFactories   = map[string]WorkflowFactory{}
)

// RegisterWorkflowContract registers a built-in workflow service contract.
func RegisterWorkflowContract(contract string, factory WorkflowFactory) {
	workflowFactoriesMu.Lock()
	defer workflowFactoriesMu.Unlock()
	workflowFactories[contract] = factory
}

func lookupWorkflowContract(contract string) WorkflowFactory {
	workflowFactoriesMu.RLock()
	defer workflowFactoriesMu.RUnlock()
	return workflowFactories[contract]
}
