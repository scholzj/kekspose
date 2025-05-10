package kekspose

import (
	"context"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestProxyConfigMap(t *testing.T) {
	keks := Keks{
		BootstrapAddress: "my-bootstrap-address:9092",
		NodeIDs:          []int32{0, 1, 2, 1000},
	}

	client := fake.NewClientset()

	proxy := Proxy{
		Client:       client,
		Namespace:    "my-namespace",
		KeksposeName: "my-kekspose",
		ProxyImage:   "my-proxy-image:latest",
		StartingPort: 10000,
		Timeout:      30000,
		Keks:         keks,
	}

	err := proxy.createConfigMap()
	assert.Nil(t, err)

	cm, err := client.CoreV1().ConfigMaps("my-namespace").Get(context.TODO(), "my-kekspose", metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, cm.Data["proxy-config.yaml"], `virtualClusters:
              - name: kekspose
                targetCluster:
                  bootstrapServers: my-bootstrap-address:9092
                gateways:
                  - name: kekspose-gateway
                    portIdentifiesNode:
                      bootstrapAddress: 127.0.0.1:10000
                      nodeIdRanges:
                        - name: brokers
                          start: 0
                          end: 1000
                logNetwork: false
                logFrames: false`)

	proxy.deleteConfigMap()

	cm, err = client.CoreV1().ConfigMaps("my-namespace").Get(context.TODO(), "my-kekspose", metav1.GetOptions{})
	assert.NotNil(t, err)
	assert.Equal(t, "configmaps \"my-kekspose\" not found", err.Error())
}

func TestProxyPod(t *testing.T) {
	keks := Keks{
		BootstrapAddress: "my-bootstrap-address:9092",
		NodeIDs:          []int32{0, 1, 2, 1000},
	}

	client := fake.NewClientset()

	proxy := Proxy{
		Client:       client,
		Namespace:    "my-namespace",
		KeksposeName: "my-kekspose",
		ProxyImage:   "my-proxy-image:latest",
		StartingPort: 10000,
		Timeout:      10,
		Keks:         keks,
	}

	// The Pod will not get ready with the fake client, so this is expected to fail. The Pod will still exist and we can assert it
	err := proxy.deployProxyPod()
	assert.NotNil(t, err)
	assert.Equal(t, "the proxy failed to get ready", err.Error())

	pod, err := client.CoreV1().Pods("my-namespace").Get(context.TODO(), "my-kekspose", metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, "my-proxy-image:latest", pod.Spec.Containers[0].Image)

	proxy.deleteProxyPod()

	pod, err = client.CoreV1().Pods("my-namespace").Get(context.TODO(), "my-kekspose", metav1.GetOptions{})
	assert.NotNil(t, err)
	assert.Equal(t, "pods \"my-kekspose\" not found", err.Error())
}
