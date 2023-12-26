package cz.scholz.kekspose;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The class responsible for configuring and deploying the proxy
 */
public class Proxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(Proxy.class);

    // Configuration template for the Kroxylicious proxy
    private static final String CONFIG_TEMPLATE = """
            virtualClusters:
              kekspose:
                targetCluster:
                  bootstrap_servers: %s
                clusterNetworkAddressConfigProvider:
                  type: PortPerBrokerClusterNetworkAddressConfigProvider
                  config:
                    bootstrapAddress: 127.0.0.1:%s
                    numberOfBrokerPorts: %s
                logNetwork: false
                logFrames: false
            """;

    private final KubernetesClient client;
    private final String namespace;
    private final String name;
    private final Integer initialPort;
    private final Integer timeoutMs;
    private final Keks keks;

    /**
     * Constructor
     *
     * @param client        Kubernetes client
     * @param namespace     Namespace of the Kafka cluster and Keksposé proxy
     * @param name          Name of the Keksposé proxy Pod
     * @param initialPort   Initial port number
     * @param timeoutMs     Timeout in milliseconds for how long we should wait for the Proxy pod readiness
     * @param keks          Keks with the Kafka cluster details
     */
    public Proxy(KubernetesClient client, String namespace, String name, Integer initialPort, Integer timeoutMs, Keks keks)    {
        this.client = client;
        this.namespace = namespace;
        this.name = name;
        this.initialPort = initialPort;
        this.timeoutMs = timeoutMs;
        this.keks = keks;
    }

    /**
     * Deploys the Proxy
     */
    public void deployProxy()   {
        LOGGER.info("Deploying the proxy");

        try {
            client.configMaps().inNamespace(namespace).resource(configMap()).create();
            client.pods().inNamespace(namespace).resource(pod()).create();
        } catch (KubernetesClientException e)   {
            if (e.getCode() == 409) {
                LOGGER.error("The Proxy Pod or ConfigMap seem to already exist.");
                throw new Keksception("The Proxy Pod or ConfigMap seem to already exist");
            }
        }

        try {
            LOGGER.info("Waiting for the proxy to become ready");
            client.pods().inNamespace(namespace).withName(name).waitUntilReady(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (KubernetesClientTimeoutException e)    {
            LOGGER.error("The Proxy Pod did not become ready");
            throw new Keksception("The Proxy Pod did not become ready");
        }
    }

    /**
     * @return  Generates the configuration ConfigMap
     */
    private ConfigMap configMap()   {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Map.of("app", "kekspose"))
                .endMetadata()
                .addToData("proxy-config.yaml", proxyConfig())
                .build();
    }

    /**
     * @return  Generates the Kroxylicious configuration
     */
    private String proxyConfig()    {
        return CONFIG_TEMPLATE.formatted(keks.bootstrapAddress(), initialPort, keks.highestNodeId() + 1);
    }

    /**
     * @return  Generates the Proxy pod
     */
    private Pod pod()   {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Map.of("app", "kekspose"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder()
                            .withName("kroxylicious")
                            .withImage("quay.io/kroxylicious/kroxylicious-developer:0.4.0")
                            .withArgs("--config", "/etc/kekspose/proxy-config.yaml")
                            .withPorts(containerPorts())
                            .withVolumeMounts(new VolumeMountBuilder().withName("proxy-config").withMountPath("/etc/kekspose/proxy-config.yaml").withSubPath("proxy-config.yaml").build())
                            .build())
                    .withVolumes(new VolumeBuilder().withName("proxy-config").withNewConfigMap().withName(name).endConfigMap().build())
                .endSpec()
                .build();
    }

    /**
     * @return  Generates the container ports
     */
    private List<ContainerPort> containerPorts()    {
        List<ContainerPort> ports = new ArrayList<>();
        ports.add(new ContainerPortBuilder().withContainerPort(initialPort).build());
        keks.nodes().forEach(n -> ports.add(new ContainerPortBuilder().withContainerPort(initialPort + n + 1).build()));
        return ports;
    }

    /**
     * Deletes the proxy
     */
    public void deleteProxy()   {
        client.pods().inNamespace(namespace).withName(name).delete();
        client.configMaps().inNamespace(namespace).withName(name).delete();
    }
}
