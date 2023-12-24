package cz.scholz.kekspose;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.LocalPortForward;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PortForward {
    private static final Logger LOGGER = LoggerFactory.getLogger(PortForward.class);

    private final KubernetesClient client;
    private final String namespace;
    private final String name;
    private final Integer initialPort;
    private final Keks keks;

    private final List<LocalPortForward> portForwards = new ArrayList<>();

    public PortForward(KubernetesClient client, String namespace, String name, Integer initialPort, Keks keks)    {
        this.client = client;
        this.namespace = namespace;
        this.name = name;
        this.initialPort = initialPort;
        this.keks = keks;
    }

    public void start() {
        // Bootstrap port
        portForwards.add(client.pods().inNamespace(namespace).withName(name).portForward(initialPort, initialPort));
        // Per broker ports
        keks.nodes().forEach(n -> {
            int port = initialPort + n + 1;
            LOGGER.info("Forwarding node {} to port {}", n, port);
            portForwards.add(client.pods().inNamespace(namespace).withName(name).portForward(port, port));
        });
    }

    public void stop()  {
        for (LocalPortForward port : portForwards)  {
            try {
                port.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close port-forward on port {}", port.getLocalPort());
            }
        }
    }
}
