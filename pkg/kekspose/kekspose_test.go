package kekspose

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapAddress(t *testing.T) {
	k := Kekspose{}
	bootstrapAddress := k.bootstrapAddress(map[int32]uint32{0: 50000, 1: 50001, 2: 50002})

	assert.Equal(t, bootstrapAddress, "localhost:50000,localhost:50001,localhost:50002")
}

func TestResolveNamespaceUsesSelectedContext(t *testing.T) {
	kubeconfig := writeTestKubeconfig(t, `apiVersion: v1
kind: Config
current-context: alpha
contexts:
- name: alpha
  context:
    cluster: test
    user: test
    namespace: alpha-ns
- name: beta
  context:
    cluster: test
    user: test
    namespace: beta-ns
clusters:
- name: test
  cluster:
    server: https://example.com
users:
- name: test
  user:
    token: test
`)

	k := Kekspose{KubeConfigPath: kubeconfig, Context: "beta"}
	err := k.resolveNamespace(k.newClientConfig())

	require.NoError(t, err)
	assert.Equal(t, "beta-ns", k.Namespace)
}

func TestResolveNamespaceUsesCurrentContextWhenNoneSpecified(t *testing.T) {
	kubeconfig := writeTestKubeconfig(t, `apiVersion: v1
kind: Config
current-context: alpha
contexts:
- name: alpha
  context:
    cluster: test
    user: test
    namespace: alpha-ns
clusters:
- name: test
  cluster:
    server: https://example.com
users:
- name: test
  user:
    token: test
`)

	k := Kekspose{KubeConfigPath: kubeconfig}
	err := k.resolveNamespace(k.newClientConfig())

	require.NoError(t, err)
	assert.Equal(t, "alpha-ns", k.Namespace)
}

func TestResolveNamespaceFailsWhenSelectedContextHasNoNamespace(t *testing.T) {
	kubeconfig := writeTestKubeconfig(t, `apiVersion: v1
kind: Config
current-context: alpha
contexts:
- name: alpha
  context:
    cluster: test
    user: test
- name: beta
  context:
    cluster: test
    user: test
clusters:
- name: test
  cluster:
    server: https://example.com
users:
- name: test
  user:
    token: test
`)

	k := Kekspose{KubeConfigPath: kubeconfig, Context: "beta"}
	err := k.resolveNamespace(k.newClientConfig())

	require.EqualError(t, err, "please use the --namespace / -n option to specify it")
}

func writeTestKubeconfig(t *testing.T, kubeconfig string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte(kubeconfig), 0o600))

	return path
}
