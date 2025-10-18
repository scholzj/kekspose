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
	"log"
	"slices"
	"strings"

	strimziapi "github.com/scholzj/strimzi-go/pkg/apis/kafka.strimzi.io/v1beta2"
	strimziclient "github.com/scholzj/strimzi-go/pkg/client/clientset/versioned"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Keks struct {
	BootstrapAddress string
	NodeIDs          []int32
}

func (k *Keks) highestNodeId() int32 {
	var highestNodeId int32

	for _, nodeId := range k.NodeIDs {
		if nodeId > highestNodeId {
			highestNodeId = nodeId
		}
	}

	return highestNodeId
}

func bakeKeks(client strimziclient.Interface, namespace string, clusterName string, listenerName string) (*Keks, error) {
	kafka, err := findKafka(client, namespace, clusterName)
	if err != nil {
		return nil, err
	}

	bootstrapAddress, err := findBootstrapAddress(kafka, clusterName, listenerName)
	if err != nil {
		return nil, err
	}

	nodes, err := findNodes(client, kafka)
	if err != nil {
		return nil, err
	}

	keks := &Keks{
		BootstrapAddress: *bootstrapAddress,
		NodeIDs:          nodes,
	}

	return keks, nil
}

func findKafka(client strimziclient.Interface, namespace string, clusterName string) (*strimziapi.Kafka, error) {
	kafka, err := client.KafkaV1beta2().Kafkas(namespace).Get(context.TODO(), clusterName, v1.GetOptions{})
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

	log.Printf("Found Kafka cluster %s in namespace %s", clusterName, namespace)

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

func findBootstrapAddress(kafka *strimziapi.Kafka, clusterName string, listenerName string) (*string, error) {
	var listener *strimziapi.GenericKafkaListener
	var err error
	if listenerName != "" {
		listener, err = findListenerByName(kafka, listenerName)
		if err != nil {
			return nil, err
		}
	} else {
		listener, err = findFirstUnencryptedListener(kafka)
		if err != nil {
			return nil, err
		}
	}

	if listener == nil {
		return nil, fmt.Errorf("failed to find listener")
	}

	var bootstrapAddress string
	if listener.Port == 9092 && listener.Name == "plain" && listener.Type == strimziapi.INTERNAL_KAFKALISTENERTYPE {
		bootstrapAddress = fmt.Sprintf("%s-kafka-bootstrap:%d", clusterName, listener.Port)
	} else if listener.Port == 9093 && listener.Name == "tls" && listener.Type == strimziapi.INTERNAL_KAFKALISTENERTYPE {
		bootstrapAddress = fmt.Sprintf("%s-kafka-bootstrap:%d", clusterName, listener.Port)
	} else if listener.Port == 9094 && listener.Name == "external" {
		bootstrapAddress = fmt.Sprintf("%s-kafka-external-bootstrap:%d", clusterName, listener.Port)
	} else {
		bootstrapAddress = fmt.Sprintf("%s-kafka-%s-bootstrap:%d", clusterName, listener.Name, listener.Port)
	}

	log.Printf("Bootstrap address %s will be used", bootstrapAddress)
	return &bootstrapAddress, nil
}

func findFirstUnencryptedListener(kafka *strimziapi.Kafka) (*strimziapi.GenericKafkaListener, error) {
	for _, listener := range kafka.Spec.Kafka.Listeners {
		if !listener.Tls {
			log.Printf("Found listener %s without TLS encryption", listener.Name)
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

func findNodes(client strimziclient.Interface, kafka *strimziapi.Kafka) ([]int32, error) {
	if len(kafka.Annotations) > 0 && kafka.Annotations["strimzi.io/node-pools"] == "enabled" {
		log.Printf("Node Pools are enabled -> getting node IDs from their status")
		var nodeIds []int32

		nodePools, err := client.KafkaV1beta2().KafkaNodePools(kafka.Namespace).List(context.TODO(), v1.ListOptions{LabelSelector: "strimzi.io/cluster=" + kafka.Name})
		if err != nil {
			return nil, fmt.Errorf("failed to list Kafka Node Pools: %v", err)
		}

		for _, nodePool := range nodePools.Items {
			if slices.Contains(nodePool.Spec.Roles, strimziapi.BROKER_PROCESSROLES) {
				if nodePool.Status != nil && len(nodePool.Status.NodeIds) > 0 {
					nodeIds = append(nodeIds, nodePool.Status.NodeIds...)
				}
			}
		}

		log.Printf("Found nodes (in broker node pools) with following IDs: %v", nodeIds)
		return nodeIds, nil
	} else {
		log.Printf("Node Pools not enabled -> calculating node IDs for %d replicas", kafka.Spec.Kafka.Replicas)
		nodeIds := make([]int32, *kafka.Spec.Kafka.Replicas)

		for i := int32(0); i < *kafka.Spec.Kafka.Replicas; i++ {
			nodeIds[i] = i
		}

		log.Printf("Generated nodes with following IDs: %v", nodeIds)
		return nodeIds, nil
	}
}
