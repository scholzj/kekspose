package keks

import (
	"context"
	"testing"

	kafkav1 "github.com/scholzj/strimzi-go/pkg/apis/kafka.strimzi.io/v1"
	"github.com/scholzj/strimzi-go/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNodePoolBasedCluster(t *testing.T) {
	kafka := &kafkav1.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
			Annotations: map[string]string{
				"strimzi.io/node-pools": "enabled",
			},
		},
		Spec: &kafkav1.KafkaSpec{
			Kafka: &kafkav1.KafkaClusterSpec{
				Version: "3.9.0",
				Listeners: []kafkav1.GenericKafkaListener{{
					Name: "plain",
					Type: kafkav1.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9092,
				}},
			},
		},
		Status: &kafkav1.KafkaStatus{
			Conditions: []kafkav1.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	volumeId := int32(0)

	nodePool1 := &kafkav1.KafkaNodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-a",
			Namespace: "my-namespace",
			Labels: map[string]string{
				"strimzi.io/cluster": "my-cluster",
			},
		},
		Spec: &kafkav1.KafkaNodePoolSpec{
			Replicas: 3,
			Roles:    []kafkav1.ProcessRoles{kafkav1.BROKER_PROCESSROLES},
			Storage: &kafkav1.Storage{
				Type: kafkav1.JBOD_STORAGETYPE,
				Volumes: []kafkav1.SingleVolumeStorage{{
					Id:   &volumeId,
					Type: kafkav1.PERSISTENT_CLAIM_SINGLEVOLUMESTORAGETYPE,
					Size: "100Gi",
				}},
			},
		},
		Status: &kafkav1.KafkaNodePoolStatus{
			NodeIds: []int32{0, 1, 2},
		},
	}

	nodePool2 := &kafkav1.KafkaNodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-b",
			Namespace: "my-namespace",
			Labels: map[string]string{
				"strimzi.io/cluster": "my-cluster",
			},
		},
		Spec: &kafkav1.KafkaNodePoolSpec{
			Replicas: 3,
			Roles:    []kafkav1.ProcessRoles{kafkav1.BROKER_PROCESSROLES, kafkav1.CONTROLLER_PROCESSROLES},
			Storage: &kafkav1.Storage{
				Type: kafkav1.JBOD_STORAGETYPE,
				Volumes: []kafkav1.SingleVolumeStorage{{
					Id:   &volumeId,
					Type: kafkav1.PERSISTENT_CLAIM_SINGLEVOLUMESTORAGETYPE,
					Size: "100Gi",
				}},
			},
		},
		Status: &kafkav1.KafkaNodePoolStatus{
			NodeIds: []int32{100, 101, 102},
		},
	}

	nodePool3 := &kafkav1.KafkaNodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-c",
			Namespace: "my-namespace",
			Labels: map[string]string{
				"strimzi.io/cluster": "my-cluster",
			},
		},
		Spec: &kafkav1.KafkaNodePoolSpec{
			Replicas: 3,
			Roles:    []kafkav1.ProcessRoles{kafkav1.CONTROLLER_PROCESSROLES},
			Storage: &kafkav1.Storage{
				Type: kafkav1.JBOD_STORAGETYPE,
				Volumes: []kafkav1.SingleVolumeStorage{{
					Id:   &volumeId,
					Type: kafkav1.PERSISTENT_CLAIM_SINGLEVOLUMESTORAGETYPE,
					Size: "100Gi",
				}},
			},
		},
		Status: &kafkav1.KafkaNodePoolStatus{
			NodeIds: []int32{1000, 1001, 1002},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	_, err = client.KafkaV1().KafkaNodePools("my-namespace").Create(context.TODO(), nodePool1, metav1.CreateOptions{})
	assert.Nil(t, err)

	_, err = client.KafkaV1().KafkaNodePools("my-namespace").Create(context.TODO(), nodePool2, metav1.CreateOptions{})
	assert.Nil(t, err)

	_, err = client.KafkaV1().KafkaNodePools("my-namespace").Create(context.TODO(), nodePool3, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := BakeKeks(client, "my-namespace", "my-cluster", "plain")
	assert.Nil(t, err)
	assert.Equal(t, map[int32]string{0: "my-cluster-pool-a-0", 1: "my-cluster-pool-a-1", 2: "my-cluster-pool-a-2", 100: "my-cluster-pool-b-100", 101: "my-cluster-pool-b-101", 102: "my-cluster-pool-b-102"}, keks.Nodes)
	assert.Equal(t, uint32(9092), keks.Port)
}

func TestUnreadyCluster(t *testing.T) {
	kafka := &kafkav1.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1.KafkaSpec{
			Kafka: &kafkav1.KafkaClusterSpec{
				Version: "3.9.0",
				Listeners: []kafkav1.GenericKafkaListener{{
					Name: "internal",
					Type: kafkav1.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9092,
				}},
			},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := BakeKeks(client, "my-namespace", "my-cluster", "internal")
	assert.NotNil(t, err)
	assert.Equal(t, "Kafka cluster my-cluster in namespace my-namespace was found, but it is not ready", err.Error())
	assert.Nil(t, keks)
}

func TestNoTlsListener(t *testing.T) {
	kafka := &kafkav1.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1.KafkaSpec{
			Kafka: &kafkav1.KafkaClusterSpec{
				Version: "3.9.0",
				Listeners: []kafkav1.GenericKafkaListener{{
					Name: "tls",
					Type: kafkav1.INTERNAL_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9093,
				}, {
					Name: "external",
					Type: kafkav1.NODEPORT_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9094,
				}},
			},
		},
		Status: &kafkav1.KafkaStatus{
			Conditions: []kafkav1.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	// Without specified listener
	keks, err := BakeKeks(client, "my-namespace", "my-cluster", "")
	assert.NotNil(t, err)
	assert.Equal(t, "no Kafka listener without TLS encryption found", err.Error())
	assert.Nil(t, keks)

	// With specified listener
	keks, err = BakeKeks(client, "my-namespace", "my-cluster", "tls")
	assert.NotNil(t, err)
	assert.Equal(t, "Kafka listener with name tls exists, but has unsupported configuration (TLS encryption is enabled)", err.Error())
	assert.Nil(t, keks)
}

func TestNonExistentListener(t *testing.T) {
	kafka := &kafkav1.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1.KafkaSpec{
			Kafka: &kafkav1.KafkaClusterSpec{
				Version: "3.9.0",
				Listeners: []kafkav1.GenericKafkaListener{{
					Name: "tls",
					Type: kafkav1.INTERNAL_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9093,
				}, {
					Name: "external",
					Type: kafkav1.NODEPORT_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9094,
				}},
			},
		},
		Status: &kafkav1.KafkaStatus{
			Conditions: []kafkav1.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := BakeKeks(client, "my-namespace", "my-cluster", "plain")
	assert.NotNil(t, err)
	assert.Equal(t, "Kafka listener with name plain was not found", err.Error())
	assert.Nil(t, keks)
}
