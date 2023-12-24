package cz.scholz.kekspose;

import java.util.Collections;
import java.util.Set;

public record Keks(String bootstrapAddress, Set<Integer> nodes) {
    public Integer highestNodeId()  {
        return Collections.max(nodes());
    }
}
