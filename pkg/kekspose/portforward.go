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
	"log"
	"net/http"
	"os"

	"github.com/scholzj/kekspose/pkg/kekspose/proksy"
	"github.com/scholzj/kekspose/pkg/kekspose/proxiedforward"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport/spdy"
)

type PortForward struct {
	KubeConfig *rest.Config
	Client     *kubernetes.Clientset
	Namespace  string
	PodName    string
	LocalPort  uint32
	RemotePort uint32
	Proxy      *proksy.Proksy
}

func (pf *PortForward) forwardPorts() {
	url := pf.Client.CoreV1().RESTClient().Post().Resource("pods").Namespace(pf.Namespace).Name(pf.PodName).SubResource("portforward").URL()

	stopCh := make(<-chan struct{})
	readyCh := make(chan struct{})

	transport, upgrader, err := spdy.RoundTripperFor(pf.KubeConfig)
	if err != nil {
		log.Fatalf("Failed to create round tripper: %v", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	fw, err := proxiedforward.New(dialer, []string{fmt.Sprintf("%d:%d", pf.LocalPort, pf.RemotePort)}, stopCh, readyCh, os.Stdout, os.Stdout, pf.Proxy)
	if err != nil {
		log.Fatalf("Failed to create port forwarder: %v", err)
	}

	log.Printf("Starting port forwarding between localhost:%d and %s:%d in namespace %s.", pf.LocalPort, pf.PodName, pf.RemotePort, pf.Namespace)

	if err := fw.ForwardPorts(); err != nil {
		log.Fatalf("Failed to forward port: %v", err)
	}
}
