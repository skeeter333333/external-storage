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

func pvName(ns, name string) string {
	return fmt.Sprintf("%s-%s", ns, name)
}

func ensureMount(server, path, mp string) error {
	if isMounted(mp) {
		return nil
	}
	if err := os.MkdirAll(mp, 0777); err != nil {
		return err
	}
	// has to be deployed as priviliged container
	cmd := exec.Command("mount", fmt.Sprintf("%s:%s", server, path), mp)
	return cmd.Run()
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

	mountPoint := filepath.Join(mountPath, server)
	err := ensureMount(server, path, mountPoint)
	if err != nil {
		return nil, fmt.Errorf("unable to mount NFS volume: " + err.Error())
	}

	pvName := pvName(options.PVC.Namespace, options.PVName)
	if err := os.MkdirAll(filepath.Join(mountPoint, pvName), 0777); err != nil {
		return nil, errors.New("unable to create directory to provision new pv: " + err.Error())
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
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
	err := ensureMount(server, path, filepath.Join(mountPath, server))
	if err != nil {
		glog.Errorf("Failed to mount %s:%s", server, path)
		return err
	}
	// PV is **not** namespaced
	pvName := pvName(volume.Spec.ClaimRef.Namespace, volume.ObjectMeta.Name)
	oldPath := filepath.Join(mountPath, server, pvName)
	archivePath := filepath.Join(mountPath, server, "archived-"+pvName)
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
