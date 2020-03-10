package com.bakdata.kafka;

import static com.bakdata.kafka.ErrorCapturingValueMapper.captureErrors;
import static java.util.Collections.emptyMap;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.kafka.util.PropertiesUtil;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopicInfo;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import picocli.CommandLine;

@SuppressWarnings("UseOfPropertiesAsHashtable")
public class S3LoaderApplication extends KafkaStreamsApplication {
    private static final AmazonS3 AMAZON_S3 = AmazonS3ClientBuilder.defaultClient();
    @SuppressWarnings("FieldMayBeFinal")
    @CommandLine.Option(names = "--s3-backed", arity = "0..1")
    private boolean useS3 = false;
    @SuppressWarnings("FieldMayBeFinal")
    @CommandLine.Option(names = "--as-document", arity = "0..1")
    private boolean asDocument = false;

    public static void main(final String[] args) {
        KafkaStreamsApplication.startApplication(new S3LoaderApplication(), args);
    }

    private static String loadFromS3(final String key) {
        final AmazonS3URI uri = new AmazonS3URI(key);
        try (final S3Object object = AMAZON_S3.getObject(uri.getBucket(), uri.getKey());
                final InputStream input = object.getObjectContent()) {
            return IOUtils.toString(input);
        } catch (final IOException e) {
            throw new RuntimeException("Error reading object " + uri + " from S3", e);
        }
    }

    private static Document asDocument(final String text) {
        return Document.newBuilder().setText(text).build();
    }

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final KStream<String, String> stream = builder.stream(this.getInputTopics());
        final KStream<String, ProcessedValue<String, String>> extracted =
                stream.mapValues(captureErrors(S3LoaderApplication::loadFromS3));

        final KStream<String, String> texts = extracted.flatMapValues(ProcessedValue::getValues);

        // write to output topic
        if (this.asDocument) {
            texts.mapValues(S3LoaderApplication::asDocument)
                    .to(this.getOutputTopic(), Produced.valueSerde(this.createOutputDocumentSerde()));
        } else {
            texts
                    .to(this.getOutputTopic(), Produced.valueSerde(this.createOutputSerde()));
        }

        // write errors to dead letter topic
        extracted
                .flatMapValues(ProcessedValue::getErrors)
                .mapValues(e -> e.createDeadLetter("Error loading document from S3"))
                .to(this.getErrorTopic(), Produced.valueSerde(this.createDeadLetterSerde()));
    }

    @Override
    public String getUniqueAppId() {
        return "s3-loader";
    }

    @Override
    protected void runStreamsApplication() {
        try (final TopicClient topicClient = TopicClient
                .create(PropertiesUtil.originals(this.getKafkaProperties()), Duration.ofSeconds(10L))) {
            final TopicInfo description = topicClient.describe(this.getInputTopic());
            final Map<String, String> config = emptyMap();
            topicClient.createIfNotExists(this.getOutputTopic(), description, config);
        }
        super.runStreamsApplication();
    }

    private Serde<String> createOutputSerde() {
        final Serde<String> serde = this.useS3 ? new S3BackedSerde<>() : Serdes.String();
        final Properties kafkaProperties = this.getKafkaProperties();
        if (this.useS3) {
            kafkaProperties.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        }
        final Map<String, Object> config = PropertiesUtil.originals(kafkaProperties);
        serde.configure(config, false);
        return serde;
    }

    private Serde<Document> createOutputDocumentSerde() {
        final Serde<Document> serde = this.useS3 ? new S3BackedSerde<>() : new SpecificAvroSerde<>();
        final Properties kafkaProperties = this.getKafkaProperties();
        if (this.useS3) {
            kafkaProperties.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        }
        final Map<String, Object> config = PropertiesUtil.originals(kafkaProperties);
        serde.configure(config, false);
        return serde;
    }

    private Serde<DeadLetter> createDeadLetterSerde() {
        final Serde<DeadLetter> serde = new SpecificAvroSerde<>();
        final Map<String, Object> config = PropertiesUtil.originals(this.getKafkaProperties());
        serde.configure(config, false);
        return serde;
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        return kafkaProperties;
    }
}
