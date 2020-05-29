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
	"os"
	"os/exec"
	"path"

	operator "github.com/pingcap-incubator/tiup-cluster/pkg/operation"
	"github.com/pingcap-incubator/tiup/pkg/set"

	"github.com/pingcap-incubator/tiup-cluster/pkg/meta"
	"github.com/pingcap-incubator/tiup-cluster/pkg/utils"
	"github.com/pingcap-incubator/tiup/components/cluster"
	tiuputils "github.com/pingcap-incubator/tiup/pkg/utils"
)

// CheckPackage checks the patched package
func CheckPackage(clusterName, comp, nodeOS, arch, packagePath, cmdRole string) error {
	var metaVersion string
	if cmdRole == CmdCluster {
		metadata, err := meta.ClusterMetadata(clusterName)
		if err != nil {
			return err
		}
		metaVersion = metadata.Version
	} else {
		metadata, err := meta.DMMetadata(clusterName)
		if err != nil {
			return err
		}
		metaVersion = metadata.Version
	}

	ver := meta.ComponentVersion(comp, metaVersion)
	repo, err := cluster.NewRepository(nodeOS, arch)
	if err != nil {
		return err
	}
	entry, err := repo.ComponentBinEntry(comp, ver)
	if err != nil {
		return err
	}

	checksum, err := utils.Checksum(packagePath)
	if err != nil {
		return err
	}
	cacheDir := meta.ClusterPath(clusterName, "cache", comp+"-"+checksum[:7])
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return err
	}
	if err := exec.Command("tar", "-xvf", packagePath, "-C", cacheDir).Run(); err != nil {
		return err
	}

	if exists := tiuputils.IsExist(path.Join(cacheDir, entry)); !exists {
		return fmt.Errorf("entry %s not found in package %s", entry, packagePath)
	}

	return nil
}

// InstancesToPatch returns instances to patch
func InstancesToPatch(topo meta.Specification, options operator.Options) ([]meta.Instance, error) {
	roleFilter := set.NewStringSet(options.Roles...)
	nodeFilter := set.NewStringSet(options.Nodes...)
	components := topo.ComponentsByStartOrder()
	components = operator.FilterComponent(components, roleFilter)

	instances := []meta.Instance{}
	comps := []string{}
	for _, com := range components {
		insts := operator.FilterInstance(com.Instances(), nodeFilter)
		if len(insts) > 0 {
			comps = append(comps, com.Name())
		}
		instances = append(instances, insts...)
	}
	if len(comps) > 1 {
		return nil, fmt.Errorf("can't patch more than one component at once: %v", comps)
	}

	if len(instances) == 0 {
		return nil, fmt.Errorf("no instance found on specifid role(%v) and nodes(%v)", options.Roles, options.Nodes)
	}

	return instances, nil
}

// OverwritePatch overwrites patch packages
func OverwritePatch(clusterName, comp, packagePath string) error {
	if err := os.MkdirAll(meta.ClusterPath(clusterName, meta.PatchDirName), 0755); err != nil {
		return err
	}
	checksum, err := utils.Checksum(packagePath)
	if err != nil {
		return err
	}
	tg := meta.ClusterPath(clusterName, meta.PatchDirName, comp+"-"+checksum[:7]+".tar.gz")
	if err := utils.CopyFile(packagePath, tg); err != nil {
		return err
	}
	if err := os.Symlink(tg, meta.ClusterPath(clusterName, meta.PatchDirName, comp+".tar.gz")); err != nil {
		return err
	}
	return nil
}
