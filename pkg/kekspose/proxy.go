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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"log"
	"time"
)

type Proxy struct {
	Client       kubernetes.Interface
	Namespace    string
	KeksposeName string
	ProxyImage   string
	StartingPort uint16
	Timeout      uint32
	Keks         Keks
}

func (p *Proxy) createConfigMap() error {
	config := fmt.Sprintf(`virtualClusters:
              kekspose:
                targetCluster:
                  bootstrap_servers: %s
                clusterNetworkAddressConfigProvider:
                  type: PortPerBrokerClusterNetworkAddressConfigProvider
                  config:
                    bootstrapAddress: 127.0.0.1:%d
                    numberOfBrokerPorts: %d
                logNetwork: false
                logFrames: false`,
		p.Keks.BootstrapAddress,
		p.StartingPort,
		p.Keks.highestNodeId()+1)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.KeksposeName,
			Namespace: p.Namespace,
		},
		Data: map[string]string{
			"proxy-config.yaml": config,
		},
	}

	_, err := p.Client.CoreV1().ConfigMaps(p.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (p *Proxy) deployProxyPod() error {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.KeksposeName,
			Namespace: p.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "kekspose",
				Image: p.ProxyImage,
				Args:  []string{"--config", "/etc/kekspose/proxy-config.yaml"},
				Ports: p.containerPorts(),
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "proxy-config",
					MountPath: "/etc/kekspose/",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name:         "proxy-config",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: p.KeksposeName}}},
			}},
		},
	}

	createdPod, err := p.Client.CoreV1().Pods(p.Namespace).Create(context.TODO(), &pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	ready, err := p.waitUntilReady(createdPod)
	if err != nil {
		return fmt.Errorf("failed to check proxy readiness: %v", err)
	}

	if !ready {
		return fmt.Errorf("the proxy failed to get ready")
	}

	return nil
}

func (p *Proxy) containerPorts() []corev1.ContainerPort {
	var ports []corev1.ContainerPort

	bootstrapPort := corev1.ContainerPort{
		ContainerPort: int32(p.StartingPort),
	}
	ports = append(ports, bootstrapPort)

	for _, nodeId := range p.Keks.NodeIDs {
		port := corev1.ContainerPort{
			ContainerPort: nodeId + int32(p.StartingPort) + 1,
		}
		ports = append(ports, port)
	}

	return ports
}

func (p *Proxy) deleteProxyPod() {
	err := p.Client.CoreV1().Pods(p.Namespace).Delete(context.TODO(), p.KeksposeName, metav1.DeleteOptions{})
	if err != nil {
		log.Fatalf("Failed to delete Keksposé proxy Pod: %v", err)
	}
}

func (p *Proxy) deleteConfigMap() {
	err := p.Client.CoreV1().ConfigMaps(p.Namespace).Delete(context.TODO(), p.KeksposeName, metav1.DeleteOptions{})
	if err != nil {
		log.Fatalf("Failed to delete Keksposé proxy ConfigMap: %v", err)
	}
}

func (p *Proxy) waitUntilReady(pod *corev1.Pod) (bool, error) {
	watchContext, watchContextCancel := context.WithTimeout(context.Background(), time.Duration(p.Timeout)*time.Millisecond)
	defer watchContextCancel()

	watcher, err := p.Client.CoreV1().Pods(p.Namespace).Watch(watchContext, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector(metav1.ObjectNameField, pod.Name).String()})
	if err != nil {
		return false, err
	}

	defer watcher.Stop()

	for {
		select {
		case event := <-watcher.ResultChan():
			if isReady(event) {
				return true, nil
			}
		case <-watchContext.Done():
			log.Print("Timed out waiting for the Keksposé Proxy Pod to be ready")
			return false, nil
		}
	}
}

func isReady(event watch.Event) bool {
	pod := event.Object.(*corev1.Pod)
	if pod.Status.Conditions != nil && len(pod.Status.Conditions) > 0 {
		for _, condition := range pod.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				log.Print("The Keksposé proxy is ready")
				return true
			}
		}

		return false
	} else {
		return false
	}
}
