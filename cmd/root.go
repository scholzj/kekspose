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

package cmd

import (
	kekspose2 "github.com/scholzj/kekspose/pkg/kekspose"
	"os"

	"github.com/spf13/cobra"
)

var kubeconfigpath string
var namespace string
var clusterName string
var listenerName string
var keksposeName string
var proxyImage string
var startingPort uint16
var timeout uint32

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "kekspose",
	Short: "Expose your Kafka cluster outside your Minikube, Kind, or Docker Desktop clusters",
	Long:  `Expose your Kafka cluster outside your Minikube, Kind, or Docker Desktop clusters`,
	Run: func(cmd *cobra.Command, args []string) {
		kekspose := kekspose2.Kekspose{
			KubeConfigPath: kubeconfigpath,
			Namespace:      namespace,
			ClusterName:    clusterName,
			ListenerName:   listenerName,
			KeksposeName:   keksposeName,
			ProxyImage:     proxyImage,
			StartingPort:   startingPort,
			Timeout:        timeout,
		}
		kekspose.ExposeKafka()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kekspose.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().StringVar(&kubeconfigpath, "kubeconfig", "", "Path to the kubeconfig file to use for Kubernetes API requests.")
	rootCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the Kafka cluster.")
	rootCmd.Flags().StringVarP(&clusterName, "cluster-name", "c", "my-cluster", "Name of the Kafka cluster.")
	rootCmd.Flags().StringVarP(&listenerName, "listener-name", "l", "", "Name of the listener that should be exposed.")
	rootCmd.Flags().StringVarP(&keksposeName, "kekspose-name", "k", "kekspose", "Name that will be used for the Keksposé ConfigMap and Pod.")
	rootCmd.Flags().StringVarP(&proxyImage, "proxy-image", "i", "ghcr.io/scholzj/kekspose:kroxylicious-0.13.0", "Container image used for the proxy (must be based on a compatible Kroxylicious container image).")
	rootCmd.Flags().Uint16VarP(&startingPort, "starting-port", "p", 50000, "The starting port number. This port number will be used for the bootstrap connection and will be used as the basis to calculate the per-broker ports.")
	rootCmd.Flags().Uint32VarP(&timeout, "timeout", "t", 300000, "Timeout for how long to wait for the Proxy Pod to become ready. In milliseconds.")
}
