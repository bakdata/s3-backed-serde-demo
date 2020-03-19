package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class S3LoaderApplicationTest {
    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder()
            .silent()
            .withSecureConnection(false)
            .build();
    private static final String INPUT_TOPIC = "INPUT";
    private static final String OUTPUT_TOPIC = "OUTPUT";
    private static final String ERROR_TOPIC = "ERROR";
    private final AmazonS3 s3Client = S3_MOCK.createS3Client();
    private final S3LoaderApplication app = createApp();
    private final TestTopology<String, String> topology = new TestTopology<>(p -> {
        this.app.setSchemaRegistryUrl(p.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        return this.app.createTopology();
    }, this.app.getKafkaProperties());

    private static S3LoaderApplication createApp() {
        final S3LoaderApplication app = new S3LoaderApplication();
        app.setInputTopics(List.of(INPUT_TOPIC));
        app.setOutputTopic(OUTPUT_TOPIC);
        app.setErrorTopic(ERROR_TOPIC);
        app.setS3(S3_MOCK.createS3Client());
        return app;
    }

    @AfterEach
    void tearDown() {
        this.topology.stop();
    }

    @Test
    void shouldLoadRecordAsText() {
        this.topology.start();
        final String bucket = "bucket";
        this.s3Client.createBucket(bucket);
        final String key = "key";
        this.s3Client.putObject(bucket, key, "foo");
        this.topology.input()
                .add("bar", "s3://" + bucket + "/" + key);
        assertThat(this.readOutput())
                .hasSize(1)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("bar"))
                .extracting(ProducerRecord::value)
                .allSatisfy(content -> assertThat(content).isEqualTo("foo"));
        this.s3Client.deleteBucket(bucket);
    }

    @Test
    void shouldLoadRecordAsDocument() {
        this.app.setAsDocument(true);
        this.topology.start();
        final String bucket = "bucket";
        this.s3Client.createBucket(bucket);
        final String key = "key";
        this.s3Client.putObject(bucket, key, "foo");
        this.topology.input()
                .add("bar", "s3://" + bucket + "/" + key);
        assertThat(this.readOutputAsDocument())
                .hasSize(1)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("bar"))
                .extracting(ProducerRecord::value)
                .allSatisfy(content -> assertThat(content.getText()).isEqualTo("foo"));
        this.s3Client.deleteBucket(bucket);
    }

    @Test
    void shouldLoadRecordAsS3BackedText() {
        final String s3BackedBucket = "s3backed";
        this.s3Client.createBucket(s3BackedBucket);
        this.app.setUseS3(true);
        final Map<String, String> streamsConfig = ImmutableMap.<String, String>builder()
                .put(AbstractS3BackedConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort())
                .put(AbstractS3BackedConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractS3BackedConfig.S3_ACCESS_KEY_CONFIG, "foo")
                .put(AbstractS3BackedConfig.S3_SECRET_KEY_CONFIG, "bar")
                .put(AbstractS3BackedConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, Boolean.toString(true))
                .put(AbstractS3BackedConfig.BASE_PATH_CONFIG, "s3://" + s3BackedBucket + "/")
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.toString(0))
                .build();
        this.app.setStreamsConfig(streamsConfig);
        this.topology.start();
        final String bucket = "bucket";
        this.s3Client.createBucket(bucket);
        final String key = "key";
        this.s3Client.putObject(bucket, key, "foo");
        this.topology.input()
                .add("bar", "s3://" + bucket + "/" + key);
        assertThat(this.readOutput())
                .hasSize(1)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("bar"))
                .extracting(ProducerRecord::value)
                .allSatisfy(content -> assertThat(content).isEqualTo("foo"));
        assertThat(this.s3Client.listObjectsV2(s3BackedBucket).getObjectSummaries())
                .hasSize(1)
                .allSatisfy(summary -> assertThat(summary.getKey()).startsWith(this.app.getOutputTopic()));
        this.s3Client.deleteBucket(bucket);
        this.s3Client.deleteBucket(s3BackedBucket);
    }

    private List<ProducerRecord<String, String>> readOutput() {
        final Serde<String> serde = this.app.createOutputStringSerde();
        return this.readOutput(serde);
    }

    private <T> List<ProducerRecord<String, T>> readOutput(final Serde<T> serde) {
        return Seq.seq(this.topology.streamOutput(this.app.getOutputTopic())
                .withValueSerde(serde))
                .toList();
    }

    private List<ProducerRecord<String, Document>> readOutputAsDocument() {
        return this.readOutput(this.app.createOutputDocumentSerde());
    }
}
