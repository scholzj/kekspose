package kekspose

import (
	"context"
	kafkav1beta2 "github.com/scholzj/strimzi-go/pkg/apis/kafka.strimzi.io/v1beta2"
	"github.com/scholzj/strimzi-go/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestZooBasedCluster(t *testing.T) {
	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: &kafkav1beta2.KafkaClusterSpec{
				Version:  "3.9.0",
				Replicas: 3,
				Listeners: []kafkav1beta2.GenericKafkaListener{{
					Name: "plain",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9092,
				}},
			},
		},
		Status: &kafkav1beta2.KafkaStatus{
			Conditions: []kafkav1beta2.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1beta2().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := bakeKeks(client, "my-namespace", "my-cluster", "plain")
	assert.Nil(t, err)
	assert.Equal(t, "my-cluster-kafka-bootstrap:9092", keks.BootstrapAddress)
	assert.Equal(t, []int32{0, 1, 2}, keks.NodeIDs)
	assert.Equal(t, int32(2), keks.highestNodeId())
}

func TestNodePoolBasedCluster(t *testing.T) {
	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
			Annotations: map[string]string{
				"strimzi.io/node-pools": "enabled",
			},
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: &kafkav1beta2.KafkaClusterSpec{
				Version: "3.9.0",
				Listeners: []kafkav1beta2.GenericKafkaListener{{
					Name: "plain",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9092,
				}},
			},
		},
		Status: &kafkav1beta2.KafkaStatus{
			Conditions: []kafkav1beta2.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	nodePool1 := &kafkav1beta2.KafkaNodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-a",
			Namespace: "my-namespace",
			Labels: map[string]string{
				"strimzi.io/cluster": "my-cluster",
			},
		},
		Spec: &kafkav1beta2.KafkaNodePoolSpec{
			Replicas: 3,
			Roles:    []kafkav1beta2.ProcessRoles{kafkav1beta2.BROKER_PROCESSROLES},
			Storage: &kafkav1beta2.Storage{
				Type: kafkav1beta2.JBOD_STORAGETYPE,
				Volumes: []kafkav1beta2.SingleVolumeStorage{{
					Id:   0,
					Type: kafkav1beta2.PERSISTENT_CLAIM_SINGLEVOLUMESTORAGETYPE,
					Size: "100Gi",
				}},
			},
		},
		Status: &kafkav1beta2.KafkaNodePoolStatus{
			NodeIds: []int32{0, 1, 2},
		},
	}

	nodePool2 := &kafkav1beta2.KafkaNodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-b",
			Namespace: "my-namespace",
			Labels: map[string]string{
				"strimzi.io/cluster": "my-cluster",
			},
		},
		Spec: &kafkav1beta2.KafkaNodePoolSpec{
			Replicas: 3,
			Roles:    []kafkav1beta2.ProcessRoles{kafkav1beta2.BROKER_PROCESSROLES, kafkav1beta2.CONTROLLER_PROCESSROLES},
			Storage: &kafkav1beta2.Storage{
				Type: kafkav1beta2.JBOD_STORAGETYPE,
				Volumes: []kafkav1beta2.SingleVolumeStorage{{
					Id:   0,
					Type: kafkav1beta2.PERSISTENT_CLAIM_SINGLEVOLUMESTORAGETYPE,
					Size: "100Gi",
				}},
			},
		},
		Status: &kafkav1beta2.KafkaNodePoolStatus{
			NodeIds: []int32{100, 101, 102},
		},
	}

	nodePool3 := &kafkav1beta2.KafkaNodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-c",
			Namespace: "my-namespace",
			Labels: map[string]string{
				"strimzi.io/cluster": "my-cluster",
			},
		},
		Spec: &kafkav1beta2.KafkaNodePoolSpec{
			Replicas: 3,
			Roles:    []kafkav1beta2.ProcessRoles{kafkav1beta2.CONTROLLER_PROCESSROLES},
			Storage: &kafkav1beta2.Storage{
				Type: kafkav1beta2.JBOD_STORAGETYPE,
				Volumes: []kafkav1beta2.SingleVolumeStorage{{
					Id:   0,
					Type: kafkav1beta2.PERSISTENT_CLAIM_SINGLEVOLUMESTORAGETYPE,
					Size: "100Gi",
				}},
			},
		},
		Status: &kafkav1beta2.KafkaNodePoolStatus{
			NodeIds: []int32{1000, 1001, 1002},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1beta2().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	_, err = client.KafkaV1beta2().KafkaNodePools("my-namespace").Create(context.TODO(), nodePool1, metav1.CreateOptions{})
	assert.Nil(t, err)

	_, err = client.KafkaV1beta2().KafkaNodePools("my-namespace").Create(context.TODO(), nodePool2, metav1.CreateOptions{})
	assert.Nil(t, err)

	_, err = client.KafkaV1beta2().KafkaNodePools("my-namespace").Create(context.TODO(), nodePool3, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := bakeKeks(client, "my-namespace", "my-cluster", "plain")
	assert.Nil(t, err)
	assert.Equal(t, "my-cluster-kafka-bootstrap:9092", keks.BootstrapAddress)
	assert.Equal(t, []int32{0, 1, 2, 100, 101, 102}, keks.NodeIDs)
	assert.Equal(t, int32(102), keks.highestNodeId())
}

func TestUnreadyCluster(t *testing.T) {
	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: &kafkav1beta2.KafkaClusterSpec{
				Version:  "3.9.0",
				Replicas: 3,
				Listeners: []kafkav1beta2.GenericKafkaListener{{
					Name: "internal",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9092,
				}},
			},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1beta2().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := bakeKeks(client, "my-namespace", "my-cluster", "internal")
	assert.NotNil(t, err)
	assert.Equal(t, "Kafka cluster my-cluster in namespace my-namespace was found, but it is not ready", err.Error())
	assert.Nil(t, keks)
}

func TestNoTlsListener(t *testing.T) {
	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: &kafkav1beta2.KafkaClusterSpec{
				Version:  "3.9.0",
				Replicas: 3,
				Listeners: []kafkav1beta2.GenericKafkaListener{{
					Name: "tls",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9093,
				}, {
					Name: "external",
					Type: kafkav1beta2.NODEPORT_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9094,
				}},
			},
		},
		Status: &kafkav1beta2.KafkaStatus{
			Conditions: []kafkav1beta2.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1beta2().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	// Without specified listener
	keks, err := bakeKeks(client, "my-namespace", "my-cluster", "")
	assert.NotNil(t, err)
	assert.Equal(t, "no Kafka listener without TLS encryption found", err.Error())
	assert.Nil(t, keks)

	// With sepcified listener
	keks, err = bakeKeks(client, "my-namespace", "my-cluster", "tls")
	assert.NotNil(t, err)
	assert.Equal(t, "Kafka listener with name tls exists, but has unsupported configuration (TLS encryption is enabled)", err.Error())
	assert.Nil(t, keks)
}

func TestNonExistentListener(t *testing.T) {
	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: &kafkav1beta2.KafkaClusterSpec{
				Version:  "3.9.0",
				Replicas: 3,
				Listeners: []kafkav1beta2.GenericKafkaListener{{
					Name: "tls",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9093,
				}, {
					Name: "external",
					Type: kafkav1beta2.NODEPORT_KAFKALISTENERTYPE,
					Tls:  true,
					Port: 9094,
				}},
			},
		},
		Status: &kafkav1beta2.KafkaStatus{
			Conditions: []kafkav1beta2.Condition{{
				Type:   "Ready",
				Status: "True",
			}},
		},
	}

	client := fake.NewSimpleClientset()
	_, err := client.KafkaV1beta2().Kafkas("my-namespace").Create(context.TODO(), kafka, metav1.CreateOptions{})
	assert.Nil(t, err)

	keks, err := bakeKeks(client, "my-namespace", "my-cluster", "plain")
	assert.NotNil(t, err)
	assert.Equal(t, "Kafka listener with name plain was not found", err.Error())
	assert.Nil(t, keks)
}

func TestBootstrapAddresses(t *testing.T) {
	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "my-namespace",
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: &kafkav1beta2.KafkaClusterSpec{
				Version:  "3.9.0",
				Replicas: 3,
				Listeners: []kafkav1beta2.GenericKafkaListener{{
					Name: "plain",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9092,
				}, {
					Name: "tls",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9093,
				}, {
					Name: "external",
					Type: kafkav1beta2.LOADBALANCER_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9094,
				}, {
					Name: "other",
					Type: kafkav1beta2.INTERNAL_KAFKALISTENERTYPE,
					Tls:  false,
					Port: 9095,
				}},
			},
		},
	}

	address, err := findBootstrapAddress(kafka, "my-cluster", "plain")
	assert.Nil(t, err)
	assert.Equal(t, "my-cluster-kafka-bootstrap:9092", *address)

	address, err = findBootstrapAddress(kafka, "my-cluster", "tls")
	assert.Nil(t, err)
	assert.Equal(t, "my-cluster-kafka-bootstrap:9093", *address)

	address, err = findBootstrapAddress(kafka, "my-cluster", "external")
	assert.Nil(t, err)
	assert.Equal(t, "my-cluster-kafka-external-bootstrap:9094", *address)

	address, err = findBootstrapAddress(kafka, "my-cluster", "other")
	assert.Nil(t, err)
	assert.Equal(t, "my-cluster-kafka-other-bootstrap:9095", *address)
}
