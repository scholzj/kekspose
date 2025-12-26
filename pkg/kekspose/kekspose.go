/*
Copyright © 2025 Jakub Scholz

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

package kekspose

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/scholzj/kekspose/pkg/kekspose/proksy"
	strimzi "github.com/scholzj/strimzi-go/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Kekspose struct {
	KubeConfigPath string
	Namespace      string
	ClusterName    string
	ListenerName   string
	StartingPort   uint32
}

func (k *Kekspose) ExposeKafka() {
	// Prepare Kubernetes client configuration
	if k.KubeConfigPath == "" {
		if os.Getenv("KUBECONFIG") != "" {
			k.KubeConfigPath = os.Getenv("KUBECONFIG")
			log.Printf("Using kubeconfig %s", k.KubeConfigPath)
		} else if home := homedir.HomeDir(); home != "" {
			k.KubeConfigPath = filepath.Join(home, ".kube", "config")
			log.Printf("Using kubeconfig %s", k.KubeConfigPath)
		}
	}

	if k.Namespace == "" && k.KubeConfigPath != "" {
		config, err := clientcmd.LoadFromFile(k.KubeConfigPath)
		if err != nil {
			log.Fatalf("Failed to parse Kubernetes client configuration: %v", err)
		}

		ns := config.Contexts[config.CurrentContext].Namespace

		if ns != "" {
			log.Printf("Using namespace %s", ns)
			k.Namespace = config.Contexts[config.CurrentContext].Namespace
		} else {
			log.Fatalf("Failed to determine the default namespace. Please use the --namespace / -n option to specify it.")
		}

	}

	kubeconfig, err := clientcmd.BuildConfigFromFlags("", k.KubeConfigPath)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client configuration: %v", err)
	}

	// Create a Kubernetes client
	kubeclient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Create a Strimzi client
	strimziclient, err := strimzi.NewForConfig(kubeconfig)
	if err != nil {
		log.Fatalf("Failed to create Strimzi client: %v", err)
	}

	// Get Kafka cluster details
	keks, err := bakeKeks(strimziclient, k.Namespace, k.ClusterName, k.ListenerName)
	if err != nil {
		log.Fatalf("Failed to find the Kafka cluster with a suitable listener: %v", err)
	}

	// Prepare the mapping
	portMapping := make(map[int32]uint32)
	nextPort := k.StartingPort

	for nodeId, _ := range keks.Nodes {
		portMapping[nodeId] = nextPort
		nextPort++
	}

	portForwarders := make([]*PortForward, 0, len(keks.Nodes))

	// LocalPort forwarders
	for nodeId, node := range keks.Nodes {
		portForwarder := PortForward{
			KubeConfig: kubeconfig,
			Client:     kubeclient,
			Namespace:  k.Namespace,
			PodName:    node,
			LocalPort:  portMapping[nodeId],
			RemotePort: keks.Port,
			Proxy:      proksy.NewProksy(nodeId, portMapping),
		}
		portForwarders = append(portForwarders, &portForwarder)
	}

	// Hook-up shutdown signal
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Print("Shutting down")
		os.Exit(0)
	}()

	log.Printf("Starting port forwarders")

	for _, pf := range portForwarders {
		go pf.forwardPorts()
	}

	log.Printf("Port forwarders should be running")
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
