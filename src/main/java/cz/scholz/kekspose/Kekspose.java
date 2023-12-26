package cz.scholz.kekspose;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * The main Keksposé class
 */
@CommandLine.Command
public class Kekspose implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kekspose.class);

    // This has to be static for the onStop method to work and delete the Kubernetes resources when Keksposé is stopped
    private static Proxy proxy = null;
    private static PortForward portForward = null;

    // Command line options
    @CommandLine.Option(names = {"-n", "--namespace"}, description = "Namespace of the Kafka cluster.")
    String namespace;

    @CommandLine.Option(names = {"-c", "--cluster-name"}, description = "Name of the Kafka cluster.", defaultValue = "my-cluster")
    String clusterName;

    @CommandLine.Option(names = {"-l", "--listener-name"}, description = "Name of the listener that should be exposed.")
    String listenerName;

    @CommandLine.Option(names = {"-p", "--starting-port"}, description = "The starting port number. This port number will be used for the bootstrap connection and will be used as the basis to calculate the per-broker ports.", defaultValue = "50000")
    Integer startingPort;

    @CommandLine.Option(names = {"-k", "--kekspose-name"}, description = "Name that will be used for the Keksposé ConfigMap and Pod.", defaultValue = "kekspose")
    String keksposeName;

    @CommandLine.Option(names = {"-t", "--timeout"}, description = "Timeout for how long to wait for the Proxy Pod to become ready. In milliseconds.", defaultValue = "300000")
    Integer timeout;

    // Injected by Quarkus
    @Inject
    KubernetesClient client;

    /**
     * Exposes the Kafka cluster
     */
    @Override
    public void run() {
        try {
            if (namespace == null) {
                // We use the default namespace if no namespace was specified
                namespace = client.getNamespace();
            }

            // Prepare everything
            Keks keks = KeksBakery.bakeKeks(client, namespace, clusterName, listenerName);
            proxy = new Proxy(client, namespace, keksposeName, startingPort, timeout, keks);
            portForward = new PortForward(client, namespace, keksposeName, startingPort, keks);

            // Run everything
            LOGGER.info("Starting the proxy");
            proxy.deployProxy();

            LOGGER.info("Starting the port forwarding");
            portForward.start();

            LOGGER.info("Everything is ready - you can now connect your Kafka client to a bootstrap server {}:{}", "127.0.0.1", startingPort);

            // Wait until we are stopped
            Quarkus.waitForExit();
        } catch (Keksception e) {
            // Error was logged already before => we just exit
            Quarkus.asyncExit(1);
        } catch (Throwable t) {
            // This was not expected => we log the exception
            LOGGER.error("Something went wrong", t);
            Quarkus.asyncExit(1);
        }
    }

    /**
     * Cleans after Keksposé during the shutdown. This is especially important to delete the Kubernetes resources.
     *
     * @param ev    Shutdown event
     */
    void onStop(@Observes ShutdownEvent ev) {
        LOGGER.info("Shutting down");

        if (portForward != null) {
            LOGGER.info("Stopping the port forwarding");
            portForward.stop();
        }

        if (proxy != null) {
            LOGGER.info("Stopping the proxy");
            proxy.deleteProxy();
        }
    }
}