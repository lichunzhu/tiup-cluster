// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/pingcap-incubator/tiup-cluster/pkg/clusterutil"
	"github.com/pingcap-incubator/tiup-cluster/pkg/executor"
	"github.com/pingcap-incubator/tiup-cluster/pkg/log"
	"github.com/pingcap-incubator/tiup-cluster/pkg/template/scripts"
)

// DMSpecification of cluster
type DMSpecification = DMTopologySpecification

// DMMasterComponent represents TiDB component.
type DMMasterComponent struct{ *DMSpecification }

// Name implements Component interface.
func (c *DMMasterComponent) Name() string {
	return ComponentDMMaster
}

// Instances implements Component interface.
func (c *DMMasterComponent) Instances() []Instance {
	ins := make([]Instance, 0, len(c.Masters))
	for _, s := range c.Masters {
		s := s
		ins = append(ins, &DMMasterInstance{
			Name: s.Name,
			instance: instance{
				InstanceSpec: s,
				name:         c.Name(),
				host:         s.Host,
				port:         s.Port,
				sshp:         s.SSHPort,
				topo:         c.DMSpecification,

				usedPorts: []int{
					s.Port,
					s.PeerPort,
				},
				usedDirs: []string{
					s.DeployDir,
					s.DataDir,
				},
				statusFn: s.Status,
			}})
	}
	return ins
}

// DMMasterInstance represent the TiDB instance
type DMMasterInstance struct {
	Name string
	instance
}

// InitConfig implement Instance interface
func (i *DMMasterInstance) InitConfig(e executor.TiOpsExecutor, clusterName, clusterVersion, deployUser string, paths DirPaths) error {
	if err := i.instance.InitConfig(e, clusterName, clusterVersion, deployUser, paths); err != nil {
		return err
	}

	topo := i.topo.GetDMSpecification()
	spec := i.InstanceSpec.(MasterSpec)
	cfg := scripts.NewDMMasterScript(
		spec.Name,
		i.GetHost(),
		paths.Deploy,
		paths.Data[0],
		paths.Log,
	).WithPort(spec.Port).WithNumaNode(spec.NumaNode).WithPeerPort(spec.PeerPort).AppendEndpoints(topo.Endpoints(deployUser)...)

	fp := filepath.Join(paths.Cache, fmt.Sprintf("run_dm-master_%s_%d.sh", i.GetHost(), i.GetPort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(paths.Deploy, "scripts", "run_dm-master.sh")
	if err := e.Transfer(fp, dst, false); err != nil {
		return err
	}
	if _, _, err := e.Execute("chmod +x "+dst, false); err != nil {
		return err
	}

	specConfig := spec.Config
	// merge config files for imported instance
	if i.IsImported() {
		configPath := ClusterPath(
			clusterName,
			AnsibleImportedConfigPath,
			fmt.Sprintf(
				"%s-%s-%d.toml",
				i.ComponentName(),
				i.GetHost(),
				i.GetPort(),
			),
		)
		importConfig, err := ioutil.ReadFile(configPath)
		if err != nil {
			return err
		}
		mergedConfig, err := mergeImported(importConfig, spec.Config)
		if err != nil {
			return err
		}
		specConfig = mergedConfig
	}

	if err := i.mergeServerConfig(e, topo.ServerConfigs.Master, specConfig, paths); err != nil {
		return err
	}

	return checkConfig(e, i.ComponentName(), i.OS(), i.Arch(), clusterVersion, i.ComponentName()+".toml", paths)
}

// ScaleConfig deploy temporary config on scaling
func (i *DMMasterInstance) ScaleConfig(e executor.TiOpsExecutor, b Specification, clusterName, clusterVersion, deployUser string, paths DirPaths) error {
	if err := i.instance.InitConfig(e, clusterName, clusterVersion, deployUser, paths); err != nil {
		return err
	}

	topo := b.GetDMSpecification()
	spec := i.InstanceSpec.(MasterSpec)
	cfg := scripts.NewDMMasterScaleScript(
		spec.Name,
		i.GetHost(),
		paths.Deploy,
		paths.Data[0],
		paths.Log,
	).WithPort(spec.Port).WithNumaNode(spec.NumaNode).WithPeerPort(spec.PeerPort).AppendEndpoints(topo.Endpoints(deployUser)...)

	fp := filepath.Join(paths.Cache, fmt.Sprintf("run_dm-master_%s_%d.sh", i.GetHost(), i.GetPort()))
	log.Infof("script path: %s", fp)
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}

	dst := filepath.Join(paths.Deploy, "scripts", "run_dm-master.sh")
	if err := e.Transfer(fp, dst, false); err != nil {
		return err
	}
	if _, _, err := e.Execute("chmod +x "+dst, false); err != nil {
		return err
	}

	specConfig := spec.Config
	return i.mergeServerConfig(e, topo.ServerConfigs.Master, specConfig, paths)
}

// DMWorkerComponent represents DM worker component.
type DMWorkerComponent struct {
	*DMSpecification
}

// Name implements Component interface.
func (c *DMWorkerComponent) Name() string {
	return ComponentDMWorker
}

// Instances implements Component interface.
func (c *DMWorkerComponent) Instances() []Instance {
	ins := make([]Instance, 0, len(c.Workers))
	for _, s := range c.Workers {
		s := s
		ins = append(ins, &DMWorkerInstance{
			Name: s.Name,
			instance: instance{
				InstanceSpec: s,
				name:         c.Name(),
				host:         s.Host,
				port:         s.Port,
				sshp:         s.SSHPort,
				topo:         c.DMSpecification,

				usedPorts: []int{
					s.Port,
				},
				usedDirs: []string{
					s.DeployDir,
					s.DataDir,
				},
				statusFn: s.Status,
			}})
	}
	return ins
}

// DMWorkerInstance represent the DM worker instance
type DMWorkerInstance struct {
	Name string
	instance
}

// InitConfig implement Instance interface
func (i *DMWorkerInstance) InitConfig(e executor.TiOpsExecutor, clusterName, clusterVersion, deployUser string, paths DirPaths) error {
	if err := i.instance.InitConfig(e, clusterName, clusterVersion, deployUser, paths); err != nil {
		return err
	}

	topo := i.topo.GetDMSpecification()
	spec := i.InstanceSpec.(WorkerSpec)
	cfg := scripts.NewDMWorkerScript(
		i.Name,
		i.GetHost(),
		paths.Deploy,
		paths.Log,
	).WithPort(spec.Port).WithNumaNode(spec.NumaNode).AppendEndpoints(topo.Endpoints(deployUser)...)
	fp := filepath.Join(paths.Cache, fmt.Sprintf("run_dm-worker_%s_%d.sh", i.GetHost(), i.GetPort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(paths.Deploy, "scripts", "run_dm-worker.sh")

	if err := e.Transfer(fp, dst, false); err != nil {
		return err
	}

	if _, _, err := e.Execute("chmod +x "+dst, false); err != nil {
		return err
	}

	specConfig := spec.Config
	// merge config files for imported instance
	if i.IsImported() {
		configPath := ClusterPath(
			clusterName,
			AnsibleImportedConfigPath,
			fmt.Sprintf(
				"%s-%s-%d.toml",
				i.ComponentName(),
				i.GetHost(),
				i.GetPort(),
			),
		)
		importConfig, err := ioutil.ReadFile(configPath)
		if err != nil {
			return err
		}
		mergedConfig, err := mergeImported(importConfig, spec.Config)
		if err != nil {
			return err
		}
		specConfig = mergedConfig
	}

	return i.mergeServerConfig(e, topo.ServerConfigs.Worker, specConfig, paths)
}

// ScaleConfig deploy temporary config on scaling
func (i *DMWorkerInstance) ScaleConfig(e executor.TiOpsExecutor, b Specification, clusterName, clusterVersion, deployUser string, paths DirPaths) error {
	s := i.instance.topo
	defer func() {
		i.instance.topo = s
	}()
	i.instance.topo = b
	return i.InitConfig(e, clusterName, clusterVersion, deployUser, paths)
}

// DMPortalComponent represents DM portal component.
type DMPortalComponent struct {
	*DMSpecification
}

// Name implements Component interface.
func (c *DMPortalComponent) Name() string {
	return ComponentDMPortal
}

// Instances implements Component interface.
func (c *DMPortalComponent) Instances() []Instance {
	ins := make([]Instance, 0, len(c.Portals))
	for _, s := range c.Portals {
		s := s
		ins = append(ins, &DMPortalInstance{
			instance: instance{
				InstanceSpec: s,
				name:         c.Name(),
				host:         s.Host,
				port:         s.Port,
				sshp:         s.SSHPort,
				topo:         c.DMSpecification,

				usedPorts: []int{
					s.Port,
				},
				usedDirs: []string{
					s.DeployDir,
					s.DataDir,
				},
				statusFn: func(_ ...string) string {
					url := fmt.Sprintf("http://%s:%d", s.Host, s.Port)
					return statusByURL(url)
				},
			}})
	}
	return ins
}

// DMPortalInstance represent the DM portal instance
type DMPortalInstance struct {
	instance
}

// InitConfig implement Instance interface
func (i *DMPortalInstance) InitConfig(e executor.TiOpsExecutor, clusterName, clusterVersion, deployUser string, paths DirPaths) error {
	if err := i.instance.InitConfig(e, clusterName, clusterVersion, deployUser, paths); err != nil {
		return err
	}

	spec := i.InstanceSpec.(PortalSpec)
	cfg := scripts.NewDMPortalScript(
		i.GetHost(),
		paths.Deploy,
		paths.Data[0],
		paths.Log,
	).WithPort(spec.Port).WithNumaNode(spec.NumaNode).WithTimeout(spec.Timeout)
	fp := filepath.Join(paths.Cache, fmt.Sprintf("run_dm-portal_%s_%d.sh", i.GetHost(), i.GetPort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(paths.Deploy, "scripts", "run_dm-portal.sh")

	if err := e.Transfer(fp, dst, false); err != nil {
		return err
	}

	if _, _, err := e.Execute("chmod +x "+dst, false); err != nil {
		return err
	}

	specConfig := spec.Config
	// merge config files for imported instance
	if i.IsImported() {
		configPath := ClusterPath(
			clusterName,
			AnsibleImportedConfigPath,
			fmt.Sprintf(
				"%s-%s-%d.toml",
				i.ComponentName(),
				i.GetHost(),
				i.GetPort(),
			),
		)
		importConfig, err := ioutil.ReadFile(configPath)
		if err != nil {
			return err
		}
		mergedConfig, err := mergeImported(importConfig, spec.Config)
		if err != nil {
			return err
		}
		specConfig = mergedConfig
	}

	return i.mergeServerConfig(e, i.topo.GetDMSpecification().ServerConfigs.Portal, specConfig, paths)
}

// ScaleConfig deploy temporary config on scaling
func (i *DMPortalInstance) ScaleConfig(e executor.TiOpsExecutor, b Specification, clusterName, clusterVersion, deployUser string, paths DirPaths) error {
	s := i.instance.topo
	defer func() {
		i.instance.topo = s
	}()
	i.instance.topo = b
	return i.InitConfig(e, clusterName, clusterVersion, deployUser, paths)
}

// GetGlobalOptions returns cluster topology
func (topo *DMSpecification) GetGlobalOptions() GlobalOptions {
	return topo.GlobalOptions
}

// GetMonitoredOptions returns MonitoredOptions
func (topo *DMSpecification) GetMonitoredOptions() MonitoredOptions {
	return topo.MonitoredOptions
}

// GetMonitors returns Monitors
func (topo *DMSpecification) GetMonitors() []PrometheusSpec {
	return topo.Monitors
}

// GetGrafana returns Grafana
func (topo *DMSpecification) GetGrafana() []GrafanaSpec {
	return topo.Grafana
}

// GetAlertManager returns Alertmanager
func (topo *DMSpecification) GetAlertManager() []AlertManagerSpec {
	return topo.Alertmanager
}

// GetClusterSpecification returns cluster topology
func (topo *DMSpecification) GetClusterSpecification() *ClusterSpecification {
	return nil
}

// GetDMSpecification returns dm topology
func (topo *DMSpecification) GetDMSpecification() *DMSpecification {
	return topo
}

// ComponentsByStopOrder return component in the order need to stop.
func (topo *DMSpecification) ComponentsByStopOrder() (comps []Component) {
	comps = topo.ComponentsByStartOrder()
	// revert order
	i := 0
	j := len(comps) - 1
	for i < j {
		comps[i], comps[j] = comps[j], comps[i]
		i++
		j--
	}
	return
}

// ComponentsByStartOrder return component in the order need to start.
func (topo *DMSpecification) ComponentsByStartOrder() (comps []Component) {
	// "dm-master", "dm-worker", "dm-portal"
	comps = append(comps, &DMMasterComponent{topo})
	comps = append(comps, &DMWorkerComponent{topo})
	comps = append(comps, &DMPortalComponent{topo})
	return
}

// ComponentsByUpdateOrder return component in the order need to be updated.
func (topo *DMSpecification) ComponentsByUpdateOrder() (comps []Component) {
	// "dm-master", "dm-worker", "dm-portal"
	comps = append(comps, &DMMasterComponent{topo})
	comps = append(comps, &DMWorkerComponent{topo})
	comps = append(comps, &DMPortalComponent{topo})
	return
}

// IterComponent iterates all components in component starting order
func (topo *DMSpecification) IterComponent(fn func(comp Component)) {
	for _, comp := range topo.ComponentsByStartOrder() {
		fn(comp)
	}
}

// IterInstance iterates all instances in component starting order
func (topo *DMSpecification) IterInstance(fn func(instance Instance)) {
	for _, comp := range topo.ComponentsByStartOrder() {
		for _, inst := range comp.Instances() {
			fn(inst)
		}
	}
}

// IterHost iterates one instance for each host
func (topo *DMSpecification) IterHost(fn func(instance Instance)) {
	hostMap := make(map[string]bool)
	for _, comp := range topo.ComponentsByStartOrder() {
		for _, inst := range comp.Instances() {
			host := inst.GetHost()
			_, ok := hostMap[host]
			if !ok {
				hostMap[host] = true
				fn(inst)
			}
		}
	}
}

// Endpoints returns the PD endpoints configurations
func (topo *DMSpecification) Endpoints(user string) []*scripts.DMMasterScript {
	var ends []*scripts.DMMasterScript
	for _, spec := range topo.Masters {
		deployDir := clusterutil.Abs(user, spec.DeployDir)
		// data dir would be empty for components which don't need it
		dataDir := spec.DataDir
		// the default data_dir is relative to deploy_dir
		if dataDir != "" && !strings.HasPrefix(dataDir, "/") {
			dataDir = filepath.Join(deployDir, dataDir)
		}
		// log dir will always be with values, but might not used by the component
		logDir := clusterutil.Abs(user, spec.LogDir)

		script := scripts.NewDMMasterScript(
			spec.Name,
			spec.Host,
			deployDir,
			dataDir,
			logDir).
			WithPort(spec.Port).
			WithPeerPort(spec.PeerPort)
		ends = append(ends, script)
	}
	return ends
}
