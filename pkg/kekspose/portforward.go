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
	"log/slog"
	"net/http"
	"net/url"

	"github.com/scholzj/kekspose/pkg/kekspose/proksy"
	"github.com/scholzj/kekspose/pkg/kekspose/proxiedforward"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport/spdy"
)

type PortForwarder struct {
	KubeConfig *rest.Config
	Url        *url.URL
	Ports      []string
	Proxy      *proksy.Proksy
	Ready      chan struct{}
	Stop       chan struct{}
}

func NewPortForwarder(kubeConfig *rest.Config, kubeClient *kubernetes.Clientset, namespace string, podName string, localPort uint32, remotePort uint32, proxy *proksy.Proksy) *PortForwarder {
	return &PortForwarder{
		KubeConfig: kubeConfig,
		Url:        kubeClient.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(podName).SubResource("portforward").URL(),
		Ports:      []string{fmt.Sprintf("%d:%d", localPort, remotePort)},
		Proxy:      proxy,
		Ready:      make(chan struct{}),
		Stop:       make(chan struct{}),
	}
}

func (pf *PortForwarder) ForwardPorts() error {
	transport, upgrader, err := spdy.RoundTripperFor(pf.KubeConfig)
	if err != nil {
		slog.Error("Failed to create round tripper", "error", err)
		return err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, pf.Url)
	fw, err := proxiedforward.New(dialer, pf.Ports, pf.Stop, pf.Ready, pf.Proxy)
	if err != nil {
		slog.Error("Failed to create port forwarder", "error", err)
		return err
	}

	if err := fw.ForwardPorts(); err != nil {
		slog.Error("Failed to forward port", "error", err)
		return err
	}

	return nil
}
