/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	linuxproc "github.com/c9s/goprocinfo/linux"
	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	provisionerNameKey = "PROVISIONER_NAME"
)

type nfsProvisioner struct {
	client kubernetes.Interface
}

const (
	mountPath = "/persistentvolumes"
)

var _ controller.Provisioner = &nfsProvisioner{}

func inMap(key string, m map[string]string) bool {
	_, ok := m[key]
	return ok
}

func isMounted(mp string) bool {
	mps, err := linuxproc.ReadMounts("/proc/mounts")
	if err != nil {
		return false
	}
	for _, m := range mps.Mounts {
		if m.MountPoint == mp {
			return true
		}
	}
	return false
}

func pvName(tenant string, stack string, service string, name string) string {
	return fmt.Sprintf("%s-%s-%s-%s", tenant, stack, service, name)
}

func mountPoint(server string, path string) string {
	return fmt.Sprintf("%s/%s/%s", mountPath, server, url.QueryEscape(path))
}

func ensureMount(server string, path string) (string, error) {
	mp := mountPoint(server, path)
	if isMounted(mp) {
		return mp, nil
	}
	if err := os.MkdirAll(mp, 0777); err != nil {
		return mp, err
	}
	// has to be deployed as priviliged container
	cmd := exec.Command("mount", fmt.Sprintf("%s:%s", server, path), mp)
	return mp, cmd.Run()
}

func (p *nfsProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}
	glog.V(4).Infof("nfs provisioner: VolumeOptions %v", options)
	params := options.Parameters
	if !(inMap("nfsPath", params) && inMap("nfsServer", params)) {
		return nil, fmt.Errorf("nfsPath and nfsServer parameters required")
	}
	server := params["nfsServer"]
	path := params["nfsPath"]
	mp, err := ensureMount(server, path)
	if err != nil {
		return nil, fmt.Errorf("unable to mount NFS volume: " + err.Error())
	}
	pvName := pvName(options.Tenant, options.Stack, options.Service, options.PVName)
	if err := os.MkdirAll(filepath.Join(mp, pvName), 0777); err != nil {
		return nil, errors.New("unable to create directory to provision new pv: " + err.Error())
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Labels: map[string]string{
				"io.wise2c.tenant":  options.Tenant,
				"io.wise2c.stack":   options.Stack,
				"io.wise2c.service": options.Service,
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{
					Server:   server,
					Path:     filepath.Join(path, pvName),
					ReadOnly: false,
				},
			},
		},
	}
	return pv, nil
}

func (p *nfsProvisioner) Delete(volume *v1.PersistentVolume) error {
	server := volume.Spec.PersistentVolumeSource.NFS.Server
	// Path include the dynamic volume name
	path := path.Dir(volume.Spec.PersistentVolumeSource.NFS.Path)
	mp, err := ensureMount(server, path)
	if err != nil {
		glog.Errorf("Failed to mount %s:%s %s", server, path, mp)
		return err
	}
	// PV is **not** namespaced
	tenant, stack, service := volume.Labels["io.wise2c.tenant"], volume.Labels["io.wise2c.stack"], volume.Labels["io.wise2c.service"]
	pvName := pvName(tenant, stack, service, volume.ObjectMeta.Name)
	oldPath := filepath.Join(mp, pvName)
	archivePath := filepath.Join(mp, "archived-"+pvName)
	glog.Infof("archiving path %s to %s", oldPath, archivePath)
	return os.Rename(oldPath, archivePath)
}

func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")

	provisionerName := os.Getenv(provisionerNameKey)
	if provisionerName == "" {
		glog.Fatalf("environment variable %s is not set! Please set it.", provisionerNameKey)
	}

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	clientNFSProvisioner := &nfsProvisioner{}
	// Start the provision controller which will dynamically provision efs NFS
	// PVs
	pc := controller.NewProvisionController(clientset, provisionerName, clientNFSProvisioner, serverVersion.GitVersion)
	pc.Run(wait.NeverStop)
}
