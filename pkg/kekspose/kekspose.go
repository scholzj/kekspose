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
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	keks2 "github.com/scholzj/kekspose/pkg/kekspose/keks"
	"github.com/scholzj/kekspose/pkg/kekspose/proksy"
	strimzi "github.com/scholzj/strimzi-go/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
			slog.Info("Found kubeconfig", "kubeconfig", k.KubeConfigPath)
		} else if home := homedir.HomeDir(); home != "" {
			k.KubeConfigPath = filepath.Join(home, ".kube", "config")
			slog.Info("Found kubeconfig", "kubeconfig", k.KubeConfigPath)
		}
	}

	if k.Namespace == "" && k.KubeConfigPath != "" {
		config, err := clientcmd.LoadFromFile(k.KubeConfigPath)
		if err != nil {
			slog.Error("Failed to parse Kubernetes client configuration", "error", err)
			return
		}

		ns := config.Contexts[config.CurrentContext].Namespace

		if ns != "" {
			slog.Info("Identified default namespace", "namespace", ns)
			k.Namespace = config.Contexts[config.CurrentContext].Namespace
		} else {
			slog.Error("Failed to determine the default namespace. Please use the --namespace / -n option to specify it.")
			return
		}

	}

	kubeconfig, err := clientcmd.BuildConfigFromFlags("", k.KubeConfigPath)
	if err != nil {
		slog.Error("Failed to create Kubernetes client configuration", "error", err)
		return
	}
	kubeconfig.WarningHandlerWithContext = rest.NoWarnings{}

	// Create a Kubernetes client
	kubeclient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		slog.Error("Failed to create Kubernetes client", "error", err)
		return
	}

	// Create a Strimzi client
	strimziclient, err := strimzi.NewForConfig(kubeconfig)
	if err != nil {
		slog.Error("Failed to create Strimzi client", "error", err)
		return
	}

	// Get Kafka cluster details
	keks, err := keks2.BakeKeks(strimziclient, k.Namespace, k.ClusterName, k.ListenerName)
	if err != nil {
		slog.Error("Failed to find the Kafka cluster with a suitable listener", "error", err)
		return
	}

	// Shutdown channels
	shutdown := make(chan struct{})
	errors := make(chan error)

	// Prepare the mapping
	portMapping := k.preparePortMapping(keks)

	// Prepare forwarders
	portForwarders := k.preparePortForwarders(kubeconfig, kubeclient, keks, portMapping)

	// Start forwarders
	for _, pf := range portForwarders {
		id := pf.Proxy.NodeId
		slog.Info("Starting port forwarding between localhost and Kubernetes", "localPort", portMapping[id], "podName", keks.Nodes[id], "remotePort", keks.Port, "namespace", k.Namespace)

		go pf.ForwardPorts()
	}

	// Wait for forwarders readiness
	for _, pf := range portForwarders {
		<-pf.Ready
	}

	slog.Info("Port forwarding is ready")
	slog.Info("Use the following address to access the Kafka cluster", "address", k.bootstrapAddress(portMapping))
	slog.Info("Press Ctrl+C to stop port forwarding")

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		slog.Info("Received Ctrl+C, stopping port forwarding")

		// Stopping port forwarders
		for _, pf := range portForwarders {
			id := pf.Proxy.NodeId
			slog.Info("Stopping port forwarding between localhost and Kubernetes", "localPort", portMapping[id], "podName", keks.Nodes[id], "remotePort", keks.Port, "namespace", k.Namespace)
			close(pf.Stop)
		}
		close(shutdown)
	}()

	// Wait for shutdown
	select {
	case <-shutdown:
		slog.Info("Shutting down")
		os.Exit(0)
	case err := <-errors:
		slog.Error("Failed forwarding ports", "error", err)
		os.Exit(1)
	}
}

func (k *Kekspose) preparePortMapping(keks *keks2.Keks) map[int32]uint32 {
	portMapping := make(map[int32]uint32)
	nextPort := k.StartingPort

	nodeIds := make([]int32, 0, len(keks.Nodes))
	for nodeId := range keks.Nodes {
		nodeIds = append(nodeIds, nodeId)
	}
	slices.Sort(nodeIds)

	for _, nodeId := range nodeIds {
		portMapping[nodeId] = nextPort
		nextPort++
	}

	return portMapping
}

func (k *Kekspose) preparePortForwarders(kubeconfig *rest.Config, kubeclient *kubernetes.Clientset, keks *keks2.Keks, portMapping map[int32]uint32) []*PortForwarder {
	portForwarders := make([]*PortForwarder, 0, len(keks.Nodes))

	nodeIds := make([]int32, 0, len(keks.Nodes))
	for nodeId := range keks.Nodes {
		nodeIds = append(nodeIds, nodeId)
	}
	slices.Sort(nodeIds)

	for _, nodeId := range nodeIds {
		node := keks.Nodes[nodeId]
		portForwarder := NewPortForwarder(kubeconfig, kubeclient, k.Namespace, node, portMapping[nodeId], keks.Port, proksy.NewProksy(nodeId, portMapping))
		portForwarders = append(portForwarders, portForwarder)
	}

	return portForwarders
}

func (k *Kekspose) bootstrapAddress(portMapping map[int32]uint32) string {
	addresses := make([]string, 0, len(portMapping))

	nodeIds := make([]int32, 0, len(portMapping))
	for nodeId := range portMapping {
		nodeIds = append(nodeIds, nodeId)
	}
	slices.Sort(nodeIds)

	for _, nodeId := range nodeIds {
		addresses = append(addresses, net.JoinHostPort("localhost", strconv.FormatUint(uint64(portMapping[nodeId]), 10)))
	}

	return strings.Join(addresses, ",")
}
