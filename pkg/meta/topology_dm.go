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
	"path/filepath"

	"github.com/creasty/defaults"
	"github.com/pingcap-incubator/tiup-cluster/pkg/api"
	"github.com/pingcap/errors"
)

type (
	// DMServerConfigs represents the server runtime configuration
	DMServerConfigs struct {
		Master map[string]interface{} `yaml:"dm-master"`
		Worker map[string]interface{} `yaml:"dm-worker"`
		Portal map[string]interface{} `yaml:"dm-portal"`
	}

	// DMTopologySpecification represents the specification of topology.yaml
	DMTopologySpecification struct {
		GlobalOptions    GlobalOptions      `yaml:"global,omitempty"`
		MonitoredOptions MonitoredOptions   `yaml:"monitored,omitempty"`
		ServerConfigs    DMServerConfigs    `yaml:"server_configs,omitempty"`
		Masters          []MasterSpec       `yaml:"dm-master_servers"`
		Workers          []WorkerSpec       `yaml:"dm-worker_servers"`
		Portals          []PortalSpec       `yaml:"dm-portal_servers"`
		Monitors         []PrometheusSpec   `yaml:"monitoring_servers"`
		Grafana          []GrafanaSpec      `yaml:"grafana_servers,omitempty"`
		Alertmanager     []AlertManagerSpec `yaml:"alertmanager_servers,omitempty"`
	}
)

// AllDMComponentNames contains the names of all dm components.
// should include all components in ComponentsByStartOrder
func AllDMComponentNames() (roles []string) {
	tp := &DMSpecification{}
	tp.IterComponent(func(c Component) {
		roles = append(roles, c.Name())
	})

	return
}

// MasterSpec represents the Master topology specification in topology.yaml
type MasterSpec struct {
	Host     string `yaml:"host"`
	SSHPort  int    `yaml:"ssh_port,omitempty"`
	Imported bool   `yaml:"imported,omitempty"`
	// Use Name to get the name with a default value if it's empty.
	Name            string                 `yaml:"name"`
	Port            int                    `yaml:"port" default:"8261"`
	PeerPort        int                    `yaml:"peer_port" default:"8291"`
	DeployDir       string                 `yaml:"deploy_dir,omitempty"`
	DataDir         string                 `yaml:"data_dir,omitempty"`
	LogDir          string                 `yaml:"log_dir,omitempty"`
	NumaNode        string                 `yaml:"numa_node,omitempty"`
	Config          map[string]interface{} `yaml:"config,omitempty"`
	ResourceControl ResourceControl        `yaml:"resource_control,omitempty"`
	Arch            string                 `yaml:"arch,omitempty"`
	OS              string                 `yaml:"os,omitempty"`
}

// Status queries current status of the instance
func (s MasterSpec) Status(masterList ...string) string {
	if len(masterList) < 1 {
		return "N/A"
	}
	masterapi := api.NewDMMasterClient(masterList, statusQueryTimeout, nil)
	isFound, isActive, isLeader, err := masterapi.GetMaster(s.Name)
	if err != nil {
		return "Down"
	}
	if !isFound {
		return "N/A"
	}
	if !isActive {
		return "Unhealthy"
	}
	res := "Healthy"
	if isLeader {
		res += "|L"
	}
	return res
}

// Role returns the component role of the instance
func (s MasterSpec) Role() string {
	return ComponentDMMaster
}

// SSH returns the host and SSH port of the instance
func (s MasterSpec) SSH() (string, int) {
	return s.Host, s.SSHPort
}

// GetMainPort returns the main port of the instance
func (s MasterSpec) GetMainPort() int {
	return s.Port
}

// IsImported returns if the node is imported from TiDB-Ansible
func (s MasterSpec) IsImported() bool {
	return s.Imported
}

// WorkerSpec represents the Master topology specification in topology.yaml
type WorkerSpec struct {
	Host     string `yaml:"host"`
	SSHPort  int    `yaml:"ssh_port,omitempty"`
	Imported bool   `yaml:"imported,omitempty"`
	// Use Name to get the name with a default value if it's empty.
	Name            string                 `yaml:"name"`
	Port            int                    `yaml:"port" default:"8262"`
	DeployDir       string                 `yaml:"deploy_dir,omitempty"`
	DataDir         string                 `yaml:"data_dir,omitempty"`
	LogDir          string                 `yaml:"log_dir,omitempty"`
	NumaNode        string                 `yaml:"numa_node,omitempty"`
	Config          map[string]interface{} `yaml:"config,omitempty"`
	ResourceControl ResourceControl        `yaml:"resource_control,omitempty"`
	Arch            string                 `yaml:"arch,omitempty"`
	OS              string                 `yaml:"os,omitempty"`
}

// Status queries current status of the instance
func (s WorkerSpec) Status(masterList ...string) string {
	if len(masterList) < 1 {
		return "N/A"
	}
	masterapi := api.NewDMMasterClient(masterList, statusQueryTimeout, nil)
	stage, err := masterapi.GetWorker(s.Name)
	if err != nil {
		return "Down"
	}
	if stage == "" {
		return "N/A"
	}
	return stage
}

// Role returns the component role of the instance
func (s WorkerSpec) Role() string {
	return ComponentDMWorker
}

// SSH returns the host and SSH port of the instance
func (s WorkerSpec) SSH() (string, int) {
	return s.Host, s.SSHPort
}

// GetMainPort returns the main port of the instance
func (s WorkerSpec) GetMainPort() int {
	return s.Port
}

// IsImported returns if the node is imported from TiDB-Ansible
func (s WorkerSpec) IsImported() bool {
	return s.Imported
}

// PortalSpec represents the Portal topology specification in topology.yaml
type PortalSpec struct {
	Host            string                 `yaml:"host"`
	SSHPort         int                    `yaml:"ssh_port,omitempty"`
	Imported        bool                   `yaml:"imported,omitempty"`
	Port            int                    `yaml:"port" default:"8280"`
	DeployDir       string                 `yaml:"deploy_dir,omitempty"`
	DataDir         string                 `yaml:"data_dir,omitempty"`
	LogDir          string                 `yaml:"log_dir,omitempty"`
	Timeout         int                    `yaml:"timeout" default:"5"`
	NumaNode        string                 `yaml:"numa_node,omitempty"`
	Config          map[string]interface{} `yaml:"config,omitempty"`
	ResourceControl ResourceControl        `yaml:"resource_control,omitempty"`
	Arch            string                 `yaml:"arch,omitempty"`
	OS              string                 `yaml:"os,omitempty"`
}

// Role returns the component role of the instance
func (s PortalSpec) Role() string {
	return ComponentDMPortal
}

// SSH returns the host and SSH port of the instance
func (s PortalSpec) SSH() (string, int) {
	return s.Host, s.SSHPort
}

// GetMainPort returns the main port of the instance
func (s PortalSpec) GetMainPort() int {
	return s.Port
}

// IsImported returns if the node is imported from TiDB-Ansible
func (s PortalSpec) IsImported() bool {
	return s.Imported
}

// UnmarshalYAML sets default values when unmarshaling the topology file
func (topo *DMTopologySpecification) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type topology DMTopologySpecification
	if err := unmarshal((*topology)(topo)); err != nil {
		return err
	}

	if err := defaults.Set(topo); err != nil {
		return errors.Trace(err)
	}

	// Set monitored options
	if topo.MonitoredOptions.DeployDir == "" {
		topo.MonitoredOptions.DeployDir = filepath.Join(topo.GlobalOptions.DeployDir,
			fmt.Sprintf("%s-%d", RoleMonitor, topo.MonitoredOptions.NodeExporterPort))
	}
	if topo.MonitoredOptions.DataDir == "" {
		topo.MonitoredOptions.DataDir = filepath.Join(topo.GlobalOptions.DataDir,
			fmt.Sprintf("%s-%d", RoleMonitor, topo.MonitoredOptions.NodeExporterPort))
	}

	if err := fillCustomDefaults(&topo.GlobalOptions, topo); err != nil {
		return err
	}

	return Validate(topo)
}

// GetMasterList returns a list of Master API hosts of the current cluster
func (topo *DMTopologySpecification) GetMasterList() []string {
	var masterList []string

	for _, master := range topo.Masters {
		masterList = append(masterList, fmt.Sprintf("%s:%d", master.Host, master.Port))
	}

	return masterList
}

// Merge returns a new TopologySpecification which sum old ones
func (topo *DMTopologySpecification) Merge(topoThat Specification) Specification {
	that := topoThat.GetDMSpecification()
	return &DMTopologySpecification{
		GlobalOptions:    topo.GlobalOptions,
		MonitoredOptions: topo.MonitoredOptions,
		ServerConfigs:    topo.ServerConfigs,
		Masters:          append(topo.Masters, that.Masters...),
		Workers:          append(topo.Workers, that.Workers...),
		Portals:          append(topo.Portals, that.Portals...),
		Monitors:         append(topo.Monitors, that.Monitors...),
		Grafana:          append(topo.Grafana, that.Grafana...),
		Alertmanager:     append(topo.Alertmanager, that.Alertmanager...),
	}
}
