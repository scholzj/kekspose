package cz.scholz.kekspose;

import java.util.Collections;
import java.util.Set;

/**
 * Record for keeping the information about the Kafka cluster we are going to expose
 *
 * @param bootstrapAddress  Bootstrap address of the Kafka cluster
 * @param nodes             Node IDs of the Kafka cluster
 */
public record Keks(String bootstrapAddress, Set<Integer> nodes) {
    // Finds the highest used Kafka node ID. This is important for configuring Kroxylicious
    public Integer highestNodeId()  {
        return Collections.max(nodes());
    }
}
