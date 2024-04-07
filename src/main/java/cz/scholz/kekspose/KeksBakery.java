package cz.scholz.kekspose;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Utility class for collecting information about the Kafka cluster we should expose and creating the Keks class with them
 */
public class KeksBakery {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeksBakery.class);

    /**
     * Creates the Keks record with all the details about the Kafka cluster
     *
     * @param client        Kubernetes client
     * @param namespace     Namespace of the Kafka cluster
     * @param clusterName   Name of the Kafka cluster
     * @param listenerName  Name of the listener that should be exposed or null if Keksposé should find a suitable listener on its own
     *
     * @return  Returns a Keks record with the details about the Kafka cluster
     */
    public static Keks bakeKeks(KubernetesClient client, String namespace, String clusterName, String listenerName)    {
        Kafka kafka = findKafka(client, namespace, clusterName);

        return new Keks(findBootstrapAddress(clusterName, findListener(kafka, listenerName)), findNodes(client, namespace, clusterName, kafka));
    }

    /**
     * Finds the Kafka cluster and returns it. Throws an exception if it is not found.
     *
     * @param client        Kubernetes client
     * @param namespace     Namespace of the Kafka cluster
     * @param clusterName   Name of the Kafka cluster
     *
     * @return  The Kafka cluster that should be exposed
     */
    private static Kafka findKafka(KubernetesClient client, String namespace, String clusterName)   {
        Kafka kafka = Crds.kafkaOperation(client).inNamespace(namespace).withName(clusterName).get();

        if (kafka == null) {
            LOGGER.error("No Kafka cluster named {} in namespace {} was found", clusterName, namespace);
            throw new Keksception("Kafka cluster not found");
        } else {
            if (isKafkaReady(kafka))    {
                LOGGER.info("Found Kafka cluster {} in namespace {}", clusterName, namespace);
                return kafka;
            } else {
                LOGGER.error("Found Kafka cluster {} in namespace {}, but it does not seem to be ready. Please run Keksposé again once the Kafka cluster is ready!", clusterName, namespace);
                throw new Keksception("Kafka cluster not ready");
            }
        }
    }

    /**
     * Checks if the Kafka cluster is ready. It does not care about the observed generation.
     *
     * @param kafka     The Kafka cluster
     *
     * @return  True if it is ready, false otherwise.
     */
    private static boolean isKafkaReady(Kafka kafka)   {
        return kafka.getStatus() != null
                && kafka.getStatus().getConditions() != null
                && kafka.getStatus().getConditions().stream().anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
    }

    /**
     * Finds a suitable listener. Throws an exception if no suitable listener is found.
     *
     * @param kafka         Kafka cluster
     * @param listenerName  Name of the listener (null if any listener can be used)
     *
     * @return  The listener that should be exposed
     */
    private static GenericKafkaListener findListener(Kafka kafka, String listenerName)  {
        Stream<GenericKafkaListener> listeners = kafka.getSpec().getKafka().getListeners().stream().filter(l -> !l.isTls());

        if (listenerName != null)   {
            listeners = listeners.filter(l -> listenerName.equals(l.getName()));
        }

        return listeners.findFirst().orElseThrow(() -> {
            if (listenerName == null) {
                LOGGER.error("No listener without TLS encryption found. Keksposé cannot expose TLS listeners.");
            } else {
                LOGGER.error("No listener named {} without TLS encryption found. Either the listener name is wrong or the listener uses TLS encryption", listenerName);
            }

            return new Keksception("No suitable interface found");
        });
    }

    /**
     * Finds the nodes of the Kafka cluster either from the Kafka resource of from the KafkaNodePools
     *
     * @param client        Kubernetes client
     * @param namespace     Namespace of the Kafka cluster
     * @param clusterName   Name of the Kafka cluster
     * @param kafka         The Kafka cluster
     *
     * @return  Set with the Node IDs used by the Kafka cluster
     */
    private static Set<Integer> findNodes(KubernetesClient client, String namespace, String clusterName, Kafka kafka) {
        Set<Integer> nodes = new HashSet<>();

        if (usesNodePools(kafka))   {
            List<KafkaNodePool> nodePools = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withLabels(Map.of("strimzi.io/cluster", clusterName)).list().getItems();

            if (nodePools == null || nodePools.isEmpty())   {
                LOGGER.error("The Kafka cluster seems to use Node Pools, but no node pool resources were found");
                throw new Keksception("No NodePools found.");
            } else {
                nodePools.stream().filter(np -> np.getStatus() != null && np.getStatus().getRoles().contains(ProcessRoles.BROKER)).forEach(np -> nodes.addAll(np.getStatus().getNodeIds()));
            }
        } else {
            int replicas = kafka.getSpec().getKafka().getReplicas();

            for (int i = 0; i < replicas; i++)  {
                nodes.add(i);
            }
        }

        LOGGER.info("Found {} Kafka nodes with ids: {}", nodes.size(), nodes);
        return nodes;
    }

    /**
     * Checks if the Kafka cluster uses node pools or not
     *
     * @param kafka Kafka cluster
     *
     * @return  True if it uses node pools, false otherwise
     */
    private static boolean usesNodePools(Kafka kafka)  {
        return kafka.getMetadata().getAnnotations() != null
                && "enabled".equals(kafka.getMetadata().getAnnotations().get("strimzi.io/node-pools"));
    }

    /**
     * Generates the bootstrap address based on the listener
     *
     * @param clusterName   Name of the Kafka cluster
     * @param listener      Listener that should be used
     *
     * @return  The bootstrap address for the Proxy configuration
     */
    private static String findBootstrapAddress(String clusterName, GenericKafkaListener listener)   {
        String bootstrapAddress;

        if (listener.getPort() == 9092 && "plain".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            bootstrapAddress = clusterName + "-kafka-bootstrap:9092";
        } else if (listener.getPort() == 9093 && "tls".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            bootstrapAddress =  clusterName + "-kafka-bootstrap:9093";
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            bootstrapAddress =  clusterName + "-kafka-external-bootstrap:9094";
        } else if (KafkaListenerType.INTERNAL == listener.getType()) {
            bootstrapAddress =  clusterName + "-kafka-bootstrap:" + listener.getPort();
        } else {
            bootstrapAddress =  clusterName + "-kafka-" + listener.getName() + "-bootstrap:" + listener.getPort();
        }

        LOGGER.info("Bootstrap address {} will be used", bootstrapAddress);
        return bootstrapAddress;
    }
}
