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

package keks

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	strimziapi "github.com/scholzj/strimzi-go/pkg/apis/kafka.strimzi.io/v1beta2"
	strimziclient "github.com/scholzj/strimzi-go/pkg/client/clientset/versioned"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Keks struct {
	Nodes map[int32]string
	Port  uint32
}

func BakeKeks(strimzi strimziclient.Interface, namespace string, clusterName string, listenerName string) (*Keks, error) {
	kafka, err := findKafka(strimzi, namespace, clusterName)
	if err != nil {
		return nil, err
	}

	port, err := findPort(kafka, listenerName)
	if err != nil {
		return nil, err
	}

	nodes, err := findNodes(strimzi, kafka)
	if err != nil {
		return nil, err
	}

	keks := &Keks{
		Port:  port,
		Nodes: nodes,
	}

	return keks, nil
}

func findKafka(strimzi strimziclient.Interface, namespace string, clusterName string) (*strimziapi.Kafka, error) {
	kafka, err := strimzi.KafkaV1beta2().Kafkas(namespace).Get(context.TODO(), clusterName, v1.GetOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			//goland:noinspection GoErrorStringFormat
			return nil, fmt.Errorf("Kafka cluster %s in namespace %s was not found: %v", clusterName, namespace, err)
		} else {
			return nil, fmt.Errorf("failed to get the Kafka cluster: %v", err)
		}
	}

	if !isKafkaReady(kafka) {
		//goland:noinspection GoErrorStringFormat
		return nil, fmt.Errorf("Kafka cluster %s in namespace %s was found, but it is not ready", clusterName, namespace)
	}

	slog.Info("Found Kafka cluster", "name", clusterName, "namespace", namespace)

	return kafka, nil
}

func isKafkaReady(kafka *strimziapi.Kafka) bool {
	if kafka.Status != nil && kafka.Status.Conditions != nil && len(kafka.Status.Conditions) > 0 {
		for _, condition := range kafka.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				return true
			}
		}

		return false
	} else {
		return false
	}
}

func findPort(kafka *strimziapi.Kafka, listenerName string) (uint32, error) {
	var listener *strimziapi.GenericKafkaListener
	var err error

	if listenerName != "" {
		listener, err = findListenerByName(kafka, listenerName)
		if err != nil {
			return 0, err
		}
	} else {
		listener, err = findFirstUnencryptedListener(kafka)
		if err != nil {
			return 0, err
		}
	}

	if listener == nil {
		return 0, fmt.Errorf("failed to find listener")
	}

	slog.Info("Found suitable port", "port", listener.Port, "listener", listener.Name)
	return uint32(listener.Port), nil
}

func findFirstUnencryptedListener(kafka *strimziapi.Kafka) (*strimziapi.GenericKafkaListener, error) {
	for _, listener := range kafka.Spec.Kafka.Listeners {
		if !listener.Tls {
			slog.Info("Found suitable listener without TLS encryption", "listener", listener.Name)
			return &listener, nil
		}
	}

	// We did not find the listener with the right name
	return nil, fmt.Errorf("no Kafka listener without TLS encryption found")
}

func findListenerByName(kafka *strimziapi.Kafka, listenerName string) (*strimziapi.GenericKafkaListener, error) {
	for _, listener := range kafka.Spec.Kafka.Listeners {
		if listener.Name == listenerName {
			if listener.Tls {
				//goland:noinspection GoErrorStringFormat
				return nil, fmt.Errorf("Kafka listener with name %s exists, but has unsupported configuration (TLS encryption is enabled)", listenerName)
			} else {
				return &listener, nil
			}
		}
	}

	// We did not find the listener with the right name
	//goland:noinspection GoErrorStringFormat
	return nil, fmt.Errorf("Kafka listener with name %s was not found", listenerName)
}

func findNodes(strimzi strimziclient.Interface, kafka *strimziapi.Kafka) (map[int32]string, error) {
	if len(kafka.Annotations) > 0 && kafka.Annotations["strimzi.io/node-pools"] == "enabled" {
		slog.Debug("Node Pools are enabled -> calculating nodes from their status")
		nodes := make(map[int32]string)

		nodePools, err := strimzi.KafkaV1beta2().KafkaNodePools(kafka.Namespace).List(context.TODO(), v1.ListOptions{LabelSelector: "strimzi.io/cluster=" + kafka.Name})
		if err != nil {
			return nil, fmt.Errorf("failed to list Kafka Node Pools: %v", err)
		}

		for _, nodePool := range nodePools.Items {
			if slices.Contains(nodePool.Spec.Roles, strimziapi.BROKER_PROCESSROLES) {
				if nodePool.Status != nil && len(nodePool.Status.NodeIds) > 0 {
					for _, nodeId := range nodePool.Status.NodeIds {
						nodes[nodeId] = fmt.Sprintf("%s-%s-%d", kafka.Name, nodePool.Name, nodeId)
					}
				}
			}
		}

		slog.Info("Found Kafka nodes", "nodes", nodes)
		return nodes, nil
	} else {
		slog.Debug("Node Pools not enabled -> calculating node IDs", "replicas", kafka.Spec.Kafka.Replicas)
		nodes := make(map[int32]string)

		for i := int32(0); i < *kafka.Spec.Kafka.Replicas; i++ {
			nodes[i] = fmt.Sprintf("%s-kafka-%d", kafka.Name, i)
		}

		slog.Info("Found Kafka nodes", "nodes", nodes)
		return nodes, nil
	}
}
