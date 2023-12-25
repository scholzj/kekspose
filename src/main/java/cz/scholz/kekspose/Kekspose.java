package cz.scholz.kekspose;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kekspose {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kekspose.class);

    private static final String NAMESPACE = "myproject";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String LISTENER_NAME = null;

    private static final String KEKSPOSE_NAME = "kekspose";
    private static final Integer STARTING_PORT = 50000;

    public static void main(String[] args) {
        //try (KubernetesClient client = new KubernetesClientBuilder().build()) {
        try {
            // Prepare everything
            KubernetesClient client = new KubernetesClientBuilder().build();
            Keks keks = KeksBakery.bakeKeks(client, NAMESPACE, CLUSTER_NAME, LISTENER_NAME);
            Proxy proxy = new Proxy(client, NAMESPACE, KEKSPOSE_NAME, STARTING_PORT, keks);
            PortForward portForward = new PortForward(client, NAMESPACE, KEKSPOSE_NAME, STARTING_PORT, keks);

            // Register shutdown hook to delete the proxy pod and terminate the port-forwards
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down");

                LOGGER.info("Stopping the port forwarding");
                portForward.stop();

                LOGGER.info("Stopping the proxy");
                proxy.deleteProxy();

                client.close();
            }));

            // Run everything
            LOGGER.info("Starting the proxy");
            proxy.deployProxy();

            LOGGER.info("Starting the port forwarding");
            portForward.start();

            LOGGER.info("Everything is ready - you can now connect your Kafka client to a bootstrap server {}:{}", "127.0.0.1", STARTING_PORT);

            Thread.currentThread().join();
        } catch (Keksception e) {
            // Error was logged already before => we just exit
            System.exit(1);
        } catch (Throwable t) {
            // This was not expected => we log the exception
            LOGGER.error("Something went wrong", t);
            System.exit(1);
        }
    }
}