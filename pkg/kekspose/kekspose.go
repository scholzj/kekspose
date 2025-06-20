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
	strimzi "github.com/scholzj/strimzi-go/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

type Kekspose struct {
	KubeConfigPath string
	Namespace      string
	ClusterName    string
	ListenerName   string
	KeksposeName   string
	ProxyImage     string
	StartingPort   uint16
	Timeout        uint32
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

	// Create Kubernetes client
	kubeclient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Create Strimzi client
	strimziclient, err := strimzi.NewForConfig(kubeconfig)
	if err != nil {
		log.Fatalf("Failed to create Strimzi client: %v", err)
	}

	// Get Kafka cluster details
	keks, err := bakeKeks(strimziclient, k.Namespace, k.ClusterName, k.ListenerName)
	if err != nil {
		log.Fatalf("Failed to find the Kafka cluster with a suitable listener: %v", err)
	}

	// Create and Deploy the proxy
	proxy := Proxy{
		Client:       kubeclient,
		Namespace:    k.Namespace,
		KeksposeName: k.KeksposeName,
		ProxyImage:   k.ProxyImage,
		StartingPort: k.StartingPort,
		Timeout:      k.Timeout,
		Keks:         *keks,
	}

	log.Printf("Deploying Keksposé proxy")

	err = proxy.createConfigMap()
	if err != nil {
		log.Fatalf("Failed to create the Proxy ConfigMap: %v", err)
	}
	defer proxy.deleteConfigMap()

	err = proxy.deployProxyPod()
	if err != nil {
		log.Fatalf("Failed to deploy the Proxy: %v", err)
	}
	defer proxy.deleteProxyPod()

	// Hook-up shutdown signal
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Print("Deleting the proxy at shutdown")
		proxy.deleteProxyPod()
		proxy.deleteConfigMap()
		os.Exit(1)
	}()

	// Start forwarding the ports
	portForwarder := PortForward{
		KubeConfig:   kubeconfig,
		Client:       kubeclient,
		Namespace:    k.Namespace,
		KeksposeName: k.KeksposeName,
		StartingPort: k.StartingPort,
		Keks:         *keks,
	}
	portForwarder.forwardPorts()
}
