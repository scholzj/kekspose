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
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/scholzj/go-kafka-protocol/messages"
	"github.com/scholzj/kekspose/pkg/kekspose"
	"github.com/spf13/cobra"
)

var kubeconfigpath string
var contextName string
var namespace string
var clusterName string
var listenerName string
var startingPort uint32
var allowUnready bool
var allowInsecureTLS bool
var verbose int
var logApis []string
var traceApis []string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "kekspose",
	Short:         "Expose your Kafka cluster outside your Minikube, Kind, or Docker Desktop clusters",
	Long:          `Expose your Kafka cluster outside your Minikube, Kind, or Docker Desktop clusters`,
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Configure the logging
		if verbose <= 0 {
			slog.SetLogLoggerLevel(slog.LevelInfo)
		} else if verbose == 1 {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		} else {
			slog.SetLogLoggerLevel(slog.Level(-10))
		}

		logKeys, err := resolveAPIKeys(logApis)
		if err != nil {
			return fmt.Errorf("invalid --log-api: %w", err)
		}
		bodyKeys, err := resolveAPIKeys(traceApis)
		if err != nil {
			return fmt.Errorf("invalid --trace-api: %w", err)
		}

		kekspose := kekspose.Kekspose{
			KubeConfigPath:   kubeconfigpath,
			Context:          contextName,
			Namespace:        namespace,
			ClusterName:      clusterName,
			ListenerName:     listenerName,
			StartingPort:     startingPort,
			AllowUnready:     allowUnready,
			AllowInsecureTLS: allowInsecureTLS,
			LogAPIKeys:       logKeys,
			BodyAPIKeys:      bodyKeys,
		}

		if err := kekspose.ExposeKafka(); err != nil {
			slog.Error("Kekspose failed", "error", err)
			return err
		}

		return nil
	},
}

// resolveAPIKeys turns a list of Kafka API names (e.g. "Metadata", "Produce") into their numeric
// API keys, matching case-insensitively against the protocol registry. It returns an error naming
// any API it does not recognise, so a typo fails fast rather than silently logging nothing.
func resolveAPIKeys(names []string) ([]int16, error) {
	if len(names) == 0 {
		return nil, nil
	}

	// Build a case-insensitive name -> key index from the generated registry.
	index := make(map[string]int16)
	for k := int16(0); k < 1000; k++ {
		if name := messages.Name(k); name != "Unknown" {
			index[strings.ToLower(name)] = k
		}
	}

	keys := make([]int16, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		key, ok := index[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unknown Kafka API %q", name)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		rootCmd.PrintErrln(err)
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
	rootCmd.Flags().StringVar(&contextName, "context", "", "Name of the Kubernetes context to use from the kubeconfig file.")
	rootCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the Kafka cluster.")
	rootCmd.Flags().StringVarP(&clusterName, "cluster-name", "c", "my-cluster", "Name of the Kafka cluster.")
	rootCmd.Flags().StringVarP(&listenerName, "listener-name", "l", "", "Name of the listener that should be exposed.")
	rootCmd.Flags().Uint32VarP(&startingPort, "starting-port", "p", 50000, "The starting port number. This port number will be used for the bootstrap connection and will be used as the basis to calculate the per-broker ports.")
	rootCmd.Flags().BoolVar(&allowUnready, "allow-unready", false, "Allow connecting to Kafka clusters even when the Kafka resource is not Ready.")
	rootCmd.Flags().BoolVar(&allowInsecureTLS, "allow-insecure-tls", false, "Allow using TLS-encrypted Kafka listeners with certificate verification disabled.")
	rootCmd.Flags().CountVarP(&verbose, "verbose", "v", "Enables verbose logging (can be repeated: -v, -vv, -vvv).")
	rootCmd.Flags().StringSliceVar(&logApis, "log-api", nil, "Restrict RPC logging to these Kafka APIs (comma-separated names, e.g. Metadata,Produce). Default: all APIs. Requires -v.")
	rootCmd.Flags().StringSliceVar(&traceApis, "trace-api", nil, "Decode and log full message bodies only for these Kafka APIs (comma-separated names, e.g. Metadata). Default: all logged APIs. Requires -vv.")
}
