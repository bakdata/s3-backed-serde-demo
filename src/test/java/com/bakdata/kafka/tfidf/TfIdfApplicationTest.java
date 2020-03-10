package com.bakdata.kafka.tfidf;

import static com.bakdata.kafka.tfidf.TfIdfApplication.ALL;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.data.Offset;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TfIdfApplicationTest {

    private static final String INPUT_TOPIC = "INPUT";
    private static final String OUTPUT_TOPIC = "OUTPUT";
    private static final Offset<Double> ONE_THOUSANDTH = Offset.offset(1.0e-3);
    private final TfIdfApplication tfIdf = createApp();
    @RegisterExtension
    TestTopologyExtension<String, Double> topology =
            new TestTopologyExtension<>(p -> {
                this.tfIdf.setSchemaRegistryUrl(p.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
                return this.tfIdf.createTopology();
            }, this.tfIdf.createKafkaProperties());

    private static TfIdfApplication createApp() {
        final TfIdfApplication tfIdf = new TfIdfApplication();
        tfIdf.setInputTopics(List.of(INPUT_TOPIC));
        tfIdf.setOutputTopic(OUTPUT_TOPIC);
        return tfIdf;
    }

    @Test
    void shouldCalculateTfIdf() {
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
    }

    private List<ProducerRecord<Integer, Long>> readDocumentCount() {
        return Seq.seq(this.topology.streamOutput(this.tfIdf.getDocumentCountTopic())
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.Long()))
                .toList();
    }

    private TestInput<String, String> input() {
        return this.topology.input(INPUT_TOPIC)
                .withValueSerde(Serdes.String());
    }

    private List<ProducerRecord<String, TfIdfScore>> readOutput() {
        return Seq.seq(this.topology.streamOutput(this.tfIdf.getOutputTopic())
                .withValueSerde(this.tfIdf.<TfIdfScore>createAvroSerde()))
                .toList();
    }

}