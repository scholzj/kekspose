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
	"fmt"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"log"
	"net/http"
	"os"
)

type PortForward struct {
	KubeConfig   *rest.Config
	Client       *kubernetes.Clientset
	Namespace    string
	KeksposeName string
	StartingPort uint16
	Keks         Keks
}

func (pf *PortForward) forwardPorts() {
	url := pf.Client.CoreV1().RESTClient().Post().Resource("pods").Namespace(pf.Namespace).Name(pf.KeksposeName).SubResource("portforward").URL()

	stopCh := make(<-chan struct{})
	readyCh := make(chan struct{})

	transport, upgrader, err := spdy.RoundTripperFor(pf.KubeConfig)
	if err != nil {
		log.Fatalf("Failed to create round tripper: %v", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	fw, err := portforward.New(dialer, pf.ports(), stopCh, readyCh, os.Stdout, os.Stdout)
	if err != nil {
		log.Fatalf("Failed to create port forwarder: %v", err)
	}

	log.Printf("Starting port forwarding. Use localhost:%d to connect to the exposed Kafka cluster.", pf.StartingPort)

	if err := fw.ForwardPorts(); err != nil {
		log.Fatalf("Failed to forward port: %v", err)
	}
}

func (pf *PortForward) ports() []string {
	var ports []string

	for _, nodeId := range pf.Keks.NodeIDs {
		ports = append(ports, fmt.Sprintf("%d:%d", int32(pf.StartingPort)+nodeId+1, int32(pf.StartingPort)+nodeId+1))
	}

	ports = append(ports, fmt.Sprintf("%d:%d", pf.StartingPort, pf.StartingPort))

	return ports
}
