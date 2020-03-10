package com.bakdata.kafka.util;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * This class creates topics that are used by Deduplication internally. Input and output topics are expected to be
 * created by the deployment pipeline.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class TopicClient implements Closeable {

    private final @NonNull AdminClient adminClient;
    private final @NonNull Duration timeout;

    public static TopicClient create(final Map<String, Object> configs, final Duration timeout) {
        return new TopicClient(AdminClient.create(configs), timeout);
    }

    /**
     * Creates a new Kafka topic with the specified number of partitions.
     *
     * Note: this function should only be used for internal topics, but not for input or output topics as they are
     * created and managed by the deployment.
     *
     * @param topicName the topic name
     * @param topicInfo configuration for number of partitions and replicationFactor
     * @param config topic configuration
     */
    public void createIfNotExists(final String topicName, final TopicInfo topicInfo, final Map<String, String> config) {
        if (this.exists(topicName)) {
            log.debug("Topic {} already exists, no need to create.", topicName);
        } else {
            this.createTopic(topicName, topicInfo.getPartitions(), topicInfo.getReplicationFactor(), config);
        }
    }

    public TopicInfo describe(final String topicName) {
        try {
            final Map<String, KafkaFuture<TopicDescription>> kafkaTopicMap =
                    this.adminClient.describeTopics(List.of(topicName)).values();
            final TopicDescription description =
                    kafkaTopicMap.get(topicName).get(this.timeout.toSeconds(), TimeUnit.SECONDS);
            final List<TopicPartitionInfo> partitions = description.partitions();
            final int replicationFactor = partitions.stream()
                    .findFirst()
                    .map(TopicPartitionInfo::replicas)
                    .map(List::size)
                    .orElseThrow(() -> new IllegalStateException("Topic " + topicName + " has no partitions"));
            return TopicInfo.builder()
                    .replicationFactor((short) replicationFactor)
                    .partitions(partitions.size())
                    .build();
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to retrieve description of topic " + topicName, e);
        }
    }

    @Override
    public void close() {
        this.adminClient.close();
    }

    public boolean exists(final String topicName) {
        try {
            final Map<String, KafkaFuture<TopicDescription>> kafkaTopicMap =
                    this.adminClient.describeTopics(List.of(topicName)).values();
            kafkaTopicMap.get(topicName).get(this.timeout.toSeconds(), TimeUnit.SECONDS);
            return true;
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            } else {
                throw new RuntimeException("Failed to check if Kafka topic " + topicName + " exists", e);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to check if Kafka topic " + topicName + " exists", e);
        } catch (final TimeoutException e) {
            throw new RuntimeException("Failed to check if Kafka topic " + topicName + " exists", e);
        }
    }

    public void createTopic(final String topicName, final int numPartitions, final short replicationFactor,
            final Map<String, String> config) {
        try {
            this.adminClient
                    .createTopics(List.of(new NewTopic(topicName, numPartitions, replicationFactor).configs(config)))
                    .all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException("Failed to create topic " + topicName, ex);
        }
    }
}
