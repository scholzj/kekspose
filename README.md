# Keksposeé

## What is Keksposé?


## How to use Keksposé?



## Frequently Asked Questions

### What happens when I scale my Kafka cluster?

You might need to restart Keksposé after scaling up your Kafka cluster or changing the IDs of the Apache Kafka nodes.

### Does Keksposé support KRaft-based Apache Kafka clusters?

Keksposé supports Kraft-based Apache Kafka cluster.
However, it exposes the broker nodes only.

### What RBAC rights do I need to run Keksposé?

Running Keksposé requires the following RBAC rights:
* Reading the Kafka and KafkaNodePool Strimzi resources
* Creating and deleting a Pod with the Kroxylicious proxy and a ConfigMap with configuration