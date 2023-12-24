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
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Proxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(Proxy.class);

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
    private final Keks keks;

    public Proxy(KubernetesClient client, String namespace, String name, Integer initialPort, Keks keks)    {
        this.client = client;
        this.namespace = namespace;
        this.name = name;
        this.initialPort = initialPort;
        this.keks = keks;
    }

    public void deployProxy()   {
        LOGGER.info("Deploying the proxy");
        client.configMaps().inNamespace(namespace).resource(configMap()).create();
        client.pods().inNamespace(namespace).resource(pod()).create();

        try {
            LOGGER.info("Waiting for the proxy to become ready");
            client.pods().inNamespace(namespace).withName(name).waitUntilReady(1, TimeUnit.MINUTES);
        } catch (KubernetesClientTimeoutException e)    {
            LOGGER.error("The Proxy Pod did not become ready");
            throw new Keksception("The Proxy Pod did not become ready");
        }
    }

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

    private String proxyConfig()    {
        return CONFIG_TEMPLATE.formatted(keks.bootstrapAddress(), initialPort, keks.highestNodeId() + 1);
    }

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

    private List<ContainerPort> containerPorts()    {
        List<ContainerPort> ports = new ArrayList<>();
        ports.add(new ContainerPortBuilder().withContainerPort(initialPort).build());
        keks.nodes().forEach(n -> ports.add(new ContainerPortBuilder().withContainerPort(initialPort + n + 1).build()));
        return ports;
    }

    public void deleteProxy()   {
        client.pods().inNamespace(namespace).withName(name).delete();
        client.configMaps().inNamespace(namespace).withName(name).delete();
    }
}
