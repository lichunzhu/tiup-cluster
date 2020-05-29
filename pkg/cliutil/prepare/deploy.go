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

package prepare

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pingcap-incubator/tiup-cluster/pkg/clusterutil"
	"github.com/pingcap-incubator/tiup-cluster/pkg/meta"
	operator "github.com/pingcap-incubator/tiup-cluster/pkg/operation"
	"github.com/pingcap-incubator/tiup-cluster/pkg/task"
)

// BuildDownloadCompTasks build download component tasks
func BuildDownloadCompTasks(version string, topo meta.Specification) []*task.StepDisplay {
	var tasks []*task.StepDisplay
	uniqueTaskList := make(map[string]struct{}) // map["comp-os-arch"]{}
	topo.IterInstance(func(inst meta.Instance) {
		key := fmt.Sprintf("%s-%s-%s", inst.ComponentName(), inst.OS(), inst.Arch())
		if _, found := uniqueTaskList[key]; !found {
			uniqueTaskList[key] = struct{}{}

			version := meta.ComponentVersion(inst.ComponentName(), version)
			t := task.
				NewBuilder().
				Download(inst.ComponentName(), inst.OS(), inst.Arch(), version).
				BuildAsStep(fmt.Sprintf("  - Download %s:%s (%s/%s)",
					inst.ComponentName(), version, inst.OS(), inst.Arch()))
			tasks = append(tasks, t)
		}
	})
	return tasks
}

// HostInfo records the host info for deployment
type HostInfo struct {
	SSH  int    // ssh port of host
	OS   string // operating system
	Arch string // cpu architecture
	// vendor string
}

// BuildMonitoredDeployTask builds monitored deploy tasks
func BuildMonitoredDeployTask(
	clusterName string,
	uniqueHosts map[string]HostInfo, // host -> ssh-port, os, arch
	globalOptions meta.GlobalOptions,
	monitoredOptions meta.MonitoredOptions,
	version string,
	gOpt operator.Options,
) (downloadCompTasks []*task.StepDisplay, deployCompTasks []*task.StepDisplay) {
	uniqueCompOSArch := make(map[string]struct{}) // comp-os-arch -> {}
	// monitoring agents
	for _, comp := range []string{meta.ComponentNodeExporter, meta.ComponentBlackboxExporter} {
		version := meta.ComponentVersion(comp, version)

		for host, info := range uniqueHosts {
			// populate unique os/arch set
			key := fmt.Sprintf("%s-%s-%s", comp, info.OS, info.Arch)
			if _, found := uniqueCompOSArch[key]; !found {
				uniqueCompOSArch[key] = struct{}{}
				downloadCompTasks = append(downloadCompTasks, task.NewBuilder().
					Download(comp, info.OS, info.Arch, version).
					BuildAsStep(fmt.Sprintf("  - Download %s:%s (%s/%s)", comp, version, info.OS, info.Arch)))
			}

			deployDir := clusterutil.Abs(globalOptions.User, monitoredOptions.DeployDir)
			// data dir would be empty for components which don't need it
			dataDir := monitoredOptions.DataDir
			// the default data_dir is relative to deploy_dir
			if dataDir != "" && !strings.HasPrefix(dataDir, "/") {
				dataDir = filepath.Join(deployDir, dataDir)
			}
			// log dir will always be with values, but might not used by the component
			logDir := clusterutil.Abs(globalOptions.User, monitoredOptions.LogDir)
			// Deploy component
			t := task.NewBuilder().
				UserSSH(host, info.SSH, globalOptions.User, gOpt.SSHTimeout).
				Mkdir(globalOptions.User, host,
					deployDir, dataDir, logDir,
					filepath.Join(deployDir, "bin"),
					filepath.Join(deployDir, "conf"),
					filepath.Join(deployDir, "scripts")).
				CopyComponent(
					comp,
					info.OS,
					info.Arch,
					version,
					host,
					deployDir,
				).
				MonitoredConfig(
					clusterName,
					comp,
					host,
					globalOptions.ResourceControl,
					monitoredOptions,
					globalOptions.User,
					meta.DirPaths{
						Deploy: deployDir,
						Data:   []string{dataDir},
						Log:    logDir,
						Cache:  meta.ClusterPath(clusterName, meta.TempConfigPath),
					},
				).
				BuildAsStep(fmt.Sprintf("  - Copy %s -> %s", comp, host))
			deployCompTasks = append(deployCompTasks, t)
		}
	}
	return
}
