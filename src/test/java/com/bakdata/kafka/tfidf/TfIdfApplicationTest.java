package com.bakdata.kafka.tfidf;

import static com.bakdata.kafka.tfidf.TfIdfApplication.ALL;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.AbstractS3BackedConfig;
import com.bakdata.kafka.Document;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.data.Offset;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TfIdfApplicationTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder()
            .silent()
            .withSecureConnection(false)
            .build();
    private static final String INPUT_TOPIC = "INPUT";
    private static final String OUTPUT_TOPIC = "OUTPUT";
    private static final Offset<Double> ONE_THOUSANDTH = Offset.offset(1.0e-3);
    private final TfIdfApplication tfIdf = createApp();
    private TestTopology<String, String> topology = null;

    private static TfIdfApplication createApp() {
        final TfIdfApplication tfIdf = new TfIdfApplication();
        tfIdf.setInputTopics(List.of(INPUT_TOPIC));
        tfIdf.setOutputTopic(OUTPUT_TOPIC);
        return tfIdf;
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    @Test
    void shouldCalculateTfIdfFromText() {
        this.start();
        this.input().add("1", "foo bar");
        final List<ProducerRecord<String, TfIdfScore>> output1 = this.readOutput();
        assertThat(output1)
                .hasSize(2)
                .allSatisfy(record -> {
                    assertThat(record.key()).isEqualTo("1");
                    // all terms occur in every document, thus idf is 0.0
                    assertThat(record.value().getTfIdf()).isEqualTo(0.0);
                })
                .extracting(ProducerRecord::value)
                .anySatisfy(score -> assertThat(score.getTerm()).isEqualTo("foo"))
                .anySatisfy(score -> assertThat(score.getTerm()).isEqualTo("bar"));
        final List<ProducerRecord<Integer, Long>> documentCount1 = this.readDocumentCount();
        assertThat(documentCount1)
                .hasSize(1)
                .anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo(ALL);
                    assertThat(record.value()).isEqualTo(1L);
                });
        final List<ProducerRecord<String, TfIdfScore>> mostImportantTerm1 = this.readMostImportantTerm();
        assertThat(mostImportantTerm1)
                .hasSize(1)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("1"))
                .extracting(ProducerRecord::value)
                .allSatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("foo");
                    // all terms occur in every document, thus idf is 0.0
                    assertThat(score.getTfIdf()).isEqualTo(0.0);
                });
        this.input().add("2", "baz baz qux");
        final List<ProducerRecord<String, TfIdfScore>> output2 = this.readOutput();
        assertThat(output2)
                .hasSize(2)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("2"))
                .extracting(ProducerRecord::value)
                .anySatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("baz");
                    // tf = 2/2; idf = ln(2/1)
                    assertThat(score.getTfIdf()).isCloseTo(0.693, ONE_THOUSANDTH);
                })
                .anySatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("qux");
                    // tf = 1/2; idf = ln(2/1)
                    assertThat(score.getTfIdf()).isCloseTo(0.347, ONE_THOUSANDTH);
                });
        final List<ProducerRecord<Integer, Long>> documentCount2 = this.readDocumentCount();
        assertThat(documentCount2)
                .hasSize(1)
                .anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo(ALL);
                    assertThat(record.value()).isEqualTo(2L);
                });
        final List<ProducerRecord<String, TfIdfScore>> mostImportantTerm2 = this.readMostImportantTerm();
        assertThat(mostImportantTerm2)
                .hasSize(1)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("2"))
                .extracting(ProducerRecord::value)
                .allSatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("baz");
                    // tf = 2/2; idf = ln(2/1)
                    assertThat(score.getTfIdf()).isCloseTo(0.693, ONE_THOUSANDTH);
                });
        this.input().add("3", "foo foo foo bar bar quux");
        final List<ProducerRecord<String, TfIdfScore>> output3 = this.readOutput();
        assertThat(output3)
                .hasSize(3)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("3"))
                .extracting(ProducerRecord::value)
                .anySatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("foo");
                    // tf = 3/3; idf = ln(3/2)
                    assertThat(score.getTfIdf()).isCloseTo(0.405, ONE_THOUSANDTH);
                })
                .anySatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("bar");
                    // tf = 2/3; idf = ln(3/2)
                    assertThat(score.getTfIdf()).isCloseTo(0.270, ONE_THOUSANDTH);
                })
                .anySatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("quux");
                    // tf = 1/3; idf = ln(3/1)
                    assertThat(score.getTfIdf()).isCloseTo(0.366, ONE_THOUSANDTH);
                });
        final List<ProducerRecord<Integer, Long>> documentCount3 = this.readDocumentCount();
        assertThat(documentCount3)
                .hasSize(1)
                .anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo(ALL);
                    assertThat(record.value()).isEqualTo(3L);
                });
        final List<ProducerRecord<String, TfIdfScore>> mostImportantTerm3 = this.readMostImportantTerm();
        assertThat(mostImportantTerm3)
                .hasSize(1)
                .allSatisfy(record -> assertThat(record.key()).isEqualTo("3"))
                .extracting(ProducerRecord::value)
                .allSatisfy(score -> {
                    assertThat(score.getTerm()).isEqualTo("foo");
                    // tf = 3/3; idf = ln(3/2)
                    assertThat(score.getTfIdf()).isCloseTo(0.405, ONE_THOUSANDTH);
                });
    }

    @Test
    void shouldCalculateTfIdfFromDocument() {
        this.tfIdf.setAsDocument(true);
        this.start();
        this.inputAsDocument().add("1", Document.newBuilder().setText("foo bar").build());
        final List<ProducerRecord<String, TfIdfScore>> output1 = this.readOutput();
        assertThat(output1)
                .hasSize(2)
                .allSatisfy(record -> {
                    assertThat(record.key()).isEqualTo("1");
                    // all terms occur in every document, thus idf is 0.0
                    assertThat(record.value().getTfIdf()).isEqualTo(0.0);
                })
                .extracting(ProducerRecord::value)
                .anySatisfy(score -> assertThat(score.getTerm()).isEqualTo("foo"))
                .anySatisfy(score -> assertThat(score.getTerm()).isEqualTo("bar"));
    }

    private void start() {
        this.topology = new TestTopology<>(p -> {
            this.tfIdf.setSchemaRegistryUrl(p.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
            return this.tfIdf.createTopology();
        }, this.tfIdf.getKafkaProperties());
        this.topology.start();
    }

    @Test
    void shouldCalculateTfIdfFromS3BackedText() {
        final String s3BackedBucket = "s3backed";
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(s3BackedBucket);
        this.tfIdf.setUseS3(true);
        final Map<String, String> streamsConfig = ImmutableMap.<String, String>builder()
                .put(AbstractS3BackedConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort())
                .put(AbstractS3BackedConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractS3BackedConfig.S3_ACCESS_KEY_CONFIG, "foo")
                .put(AbstractS3BackedConfig.S3_SECRET_KEY_CONFIG, "bar")
                .put(AbstractS3BackedConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, Boolean.toString(true))
                .put(AbstractS3BackedConfig.BASE_PATH_CONFIG, "s3://" + s3BackedBucket + "/")
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.toString(0))
                .build();
        this.tfIdf.setStreamsConfig(streamsConfig);
        this.start();
        this.input().add("1", "foo bar");
        final List<ProducerRecord<String, TfIdfScore>> output1 = this.readOutput();
        assertThat(output1)
                .hasSize(2)
                .allSatisfy(record -> {
                    assertThat(record.key()).isEqualTo("1");
                    // all terms occur in every document, thus idf is 0.0
                    assertThat(record.value().getTfIdf()).isEqualTo(0.0);
                })
                .extracting(ProducerRecord::value)
                .anySatisfy(score -> assertThat(score.getTerm()).isEqualTo("foo"))
                .anySatisfy(score -> assertThat(score.getTerm()).isEqualTo("bar"));
        assertThat(s3Client.listObjectsV2(s3BackedBucket).getObjectSummaries())
                .hasSize(1)
                .allSatisfy(summary -> assertThat(summary.getKey()).startsWith(this.tfIdf.getInputTopic()));
        s3Client.deleteBucket(s3BackedBucket);
    }

    private List<ProducerRecord<Integer, Long>> readDocumentCount() {
        return Seq.seq(this.topology.streamOutput(this.tfIdf.getDocumentCountTopic())
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.Long()))
                .toList();
    }

    private TestInput<String, String> input() {
        return this.topology.input(INPUT_TOPIC);
    }

    @SuppressWarnings("unchecked")
    private TestInput<String, Document> inputAsDocument() {
        final Serde<Document> valueSerde = new StreamsConfig(this.tfIdf.getKafkaProperties()).defaultValueSerde();
        return this.topology.input(INPUT_TOPIC)
                .withValueSerde(valueSerde);
    }

    private List<ProducerRecord<String, TfIdfScore>> readOutput() {
        return Seq.seq(this.topology.streamOutput(this.tfIdf.getOutputTopic())
                .withValueSerde(this.tfIdf.<TfIdfScore>createAvroSerde()))
                .toList();
    }

    private List<ProducerRecord<String, TfIdfScore>> readMostImportantTerm() {
        return Seq.seq(this.topology.tableOutput(this.tfIdf.getMostImportantTermTopic())
                .withValueSerde(this.tfIdf.<TfIdfScore>createAvroSerde()))
                .toList();
    }

}