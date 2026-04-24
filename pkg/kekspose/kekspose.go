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
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	KubeConfigPath   string
	Context          string
	Namespace        string
	ClusterName      string
	ListenerName     string
	StartingPort     uint32
	AllowUnready     bool
	AllowInsecureTLS bool
}

func (k *Kekspose) ExposeKafka() error {
	k.resolveKubeConfigPath()
	clientConfig := k.newClientConfig()

	if err := k.resolveNamespace(); err != nil {
		return fmt.Errorf("failed to determine the namespace: %w", err)
	}

	kubeconfig, err := clientConfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client configuration: %w", err)
	}
	kubeconfig.WarningHandlerWithContext = rest.NoWarnings{}

	// Create a Kubernetes client
	kubeclient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Create a Strimzi client
	strimziclient, err := strimzi.NewForConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to create Strimzi client: %w", err)
	}

	// Get Kafka cluster details
	keks, err := keks2.BakeKeks(strimziclient, k.Namespace, k.ClusterName, k.ListenerName, k.AllowUnready, k.AllowInsecureTLS)
	if err != nil {
		return fmt.Errorf("failed to find the Kafka cluster with a suitable listener: %w", err)
	}
	if keks.TLS {
		slog.Warn("Using TLS upstream with certificate verification disabled", "listenerName", keks.ListenerName, "overrideFlag", "--allow-insecure-tls")
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	errors := make(chan error, len(keks.Nodes))

	// Prepare the mapping
	portMapping := k.preparePortMapping(keks)

	// Prepare forwarders
	portForwarders := k.preparePortForwarders(kubeconfig, kubeclient, keks, portMapping)

	var stopOnce sync.Once
	stopPortForwarders := func() {
		stopOnce.Do(func() {
			for _, pf := range portForwarders {
				id := pf.Proxy.NodeId
				slog.Info("Stopping port forwarding between localhost and Kubernetes", "localPort", portMapping[id], "podName", keks.Nodes[id], "remotePort", keks.Port, "namespace", k.Namespace)
				close(pf.Stop)
			}
		})
	}

	// Start forwarders
	for _, pf := range portForwarders {
		id := pf.Proxy.NodeId
		slog.Info("Starting port forwarding between localhost and Kubernetes", "localPort", portMapping[id], "podName", keks.Nodes[id], "remotePort", keks.Port, "namespace", k.Namespace)

		go func(pf *PortForwarder) {
			if err := pf.ForwardPorts(); err != nil {
				errors <- err
			}
		}(pf)
	}

	// Wait for forwarders readiness
	for _, pf := range portForwarders {
		select {
		case <-pf.Ready:
		case err := <-errors:
			stopPortForwarders()
			return fmt.Errorf("failed forwarding ports: %w", err)
		}
	}

	slog.Info("Port forwarding is ready")
	slog.Info("Use the following address to access the Kafka cluster", "address", k.bootstrapAddress(portMapping))
	slog.Info("Press Ctrl+C to stop port forwarding")

	// Wait for shutdown
	select {
	case <-ctx.Done():
		slog.Info("Received shutdown signal, stopping port forwarding")
		stopPortForwarders()
		slog.Info("Shutting down")
		return nil
	case err := <-errors:
		stopPortForwarders()
		return fmt.Errorf("failed forwarding ports: %w", err)
	}
}

func (k *Kekspose) resolveKubeConfigPath() {
	if k.KubeConfigPath == "" {
		if os.Getenv("KUBECONFIG") != "" {
			k.KubeConfigPath = os.Getenv("KUBECONFIG")
			slog.Info("Found kubeconfig", "kubeconfig", k.KubeConfigPath)
		} else if home := homedir.HomeDir(); home != "" {
			k.KubeConfigPath = filepath.Join(home, ".kube", "config")
			slog.Info("Found kubeconfig", "kubeconfig", k.KubeConfigPath)
		}
	}
}

func (k *Kekspose) newClientConfig() clientcmd.ClientConfig {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()

	if k.KubeConfigPath != "" {
		loadingRules.ExplicitPath = k.KubeConfigPath
	}

	overrides := &clientcmd.ConfigOverrides{}

	if k.Context != "" {
		overrides.CurrentContext = k.Context
		slog.Info("Using Kubernetes context", "context", k.Context)
	}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
}

func (k *Kekspose) resolveNamespace() error {
	if k.Namespace != "" {
		return nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if k.KubeConfigPath != "" {
		loadingRules.ExplicitPath = k.KubeConfigPath
	}

	config, err := loadingRules.Load()
	if err != nil {
		return err
	}

	contextName := config.CurrentContext
	if k.Context != "" {
		contextName = k.Context
	}

	if contextName == "" {
		return fmt.Errorf("no Kubernetes context selected. Please use the --context option or configure a current context in kubeconfig")
	}

	context, found := config.Contexts[contextName]
	if !found {
		return fmt.Errorf("Kubernetes context %s was not found in kubeconfig", contextName)
	}

	if context.Namespace == "" {
		return fmt.Errorf("please use the --namespace / -n option to specify it")
	}

	slog.Info("Identified default namespace", "namespace", context.Namespace, "context", contextName)
	k.Namespace = context.Namespace

	return nil
}

func (k *Kekspose) preparePortMapping(keks *keks2.Keks) map[int32]uint32 {
	portMapping := make(map[int32]uint32)
	nextPort := k.StartingPort

	for _, nodeId := range sortedNodeIDs(keks.Nodes) {
		portMapping[nodeId] = nextPort
		nextPort++
	}

	return portMapping
}

func (k *Kekspose) preparePortForwarders(kubeconfig *rest.Config, kubeclient *kubernetes.Clientset, keks *keks2.Keks, portMapping map[int32]uint32) []*PortForwarder {
	portForwarders := make([]*PortForwarder, 0, len(keks.Nodes))

	for _, nodeId := range sortedNodeIDs(keks.Nodes) {
		node := keks.Nodes[nodeId]
		portForwarder := NewPortForwarder(kubeconfig, kubeclient, k.Namespace, node, portMapping[nodeId], keks.Port, keks.TLS, proksy.NewProksy(nodeId, portMapping))
		portForwarders = append(portForwarders, portForwarder)
	}

	return portForwarders
}

func (k *Kekspose) bootstrapAddress(portMapping map[int32]uint32) string {
	addresses := make([]string, 0, len(portMapping))

	for _, nodeId := range sortedNodeIDs(portMapping) {
		addresses = append(addresses, net.JoinHostPort("localhost", strconv.FormatUint(uint64(portMapping[nodeId]), 10)))
	}

	return strings.Join(addresses, ",")
}

func sortedNodeIDs[T any](nodes map[int32]T) []int32 {
	nodeIDs := make([]int32, 0, len(nodes))
	for nodeID := range nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	slices.Sort(nodeIDs)

	return nodeIDs
}
