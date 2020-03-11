package com.bakdata.kafka;

import static com.bakdata.kafka.util.PropertiesUtil.originals;
import static java.util.Collections.emptyMap;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopicInfo;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@SuppressWarnings({"UseOfPropertiesAsHashtable", "ObjectAllocationInLoop"})
@Data
public class S3ProducerApplication implements Runnable {
    private static final AmazonS3 AMAZON_S3 = AmazonS3ClientBuilder.defaultClient();
    @CommandLine.Option(names = "--brokers", required = true)
    private String brokers;
    @CommandLine.Option(names = "--output-topic", required = true)
    private String outputTopic;
    @CommandLine.Option(names = "--s3-path", required = true)
    private String s3Path;

    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new S3ProducerApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBrokers());
        this.createTopics(properties);
        try (final Producer<String, String> producer = new KafkaProducer<>(properties)) {
            final AmazonS3URI uri = new AmazonS3URI(this.s3Path);
            final String bucket = uri.getBucket();
            final String prefix = uri.getKey();
            final ListObjectsV2Result initialObjects = AMAZON_S3.listObjectsV2(bucket, prefix);
            this.sendBatch(producer, initialObjects);
            while (initialObjects.isTruncated()) {
                final ListObjectsV2Request request = new ListObjectsV2Request()
                        .withContinuationToken(initialObjects.getNextContinuationToken())
                        .withBucketName(bucket)
                        .withPrefix(prefix);
                final ListObjectsV2Result objects = AMAZON_S3.listObjectsV2(request);
                this.sendBatch(producer, objects);
            }
        }
    }

    private void createTopics(final Properties properties) {
        try (final TopicClient topicClient = TopicClient.create(originals(properties), Duration.ofSeconds(10L))) {
            final TopicInfo description = TopicInfo.builder()
                    .partitions(3)
                    .replicationFactor((short) 1)
                    .build();
            final Map<String, String> config = emptyMap();
            topicClient.createIfNotExists(this.getOutputTopic(), description, config);
        }
    }

    private void sendBatch(final Producer<? super String, ? super String> producer, final ListObjectsV2Result objects) {
        final List<S3ObjectSummary> summaries = objects.getObjectSummaries();
        for (final S3ObjectSummary summary : summaries) {
            final String uri = "s3://" + summary.getBucketName() + "/" + summary.getKey();
            producer.send(new ProducerRecord<>(this.getOutputTopic(), uri, uri));
        }
    }
}
