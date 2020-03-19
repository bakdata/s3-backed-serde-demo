package com.bakdata.kafka.tfidf;

import static com.bakdata.kafka.util.PropertiesUtil.originals;
import static java.util.Collections.emptyMap;

import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.common_kafka_streams.util.TopicClient;
import com.bakdata.common_kafka_streams.util.TopicSettings;
import com.bakdata.kafka.Document;
import com.bakdata.kafka.S3BackedSerde;
import com.bakdata.kafka.S3BackedSerdeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import picocli.CommandLine.Option;

@Setter
@SuppressWarnings("UseOfPropertiesAsHashtable")
@Slf4j
public class TfIdfApplication extends KafkaStreamsApplication {
    static final int ALL = 0;
    private static final String DOCUMENT_COUNT_TOPIC_SUFFIX = "-document-counts";
    private static final String TERM_FREQUENCIES_TOPIC_SUFFIX = "-term-frequencies";
    private static final String MOST_IMPORTANT_TERM_TOPIC_SUFFIX = "-most-important-term";
    private static final String TERM_OCCURRENCES_STATE_STORE_NAME = "term-occurrences";
    private static final String DOCUMENT_COUNT_STATE_STORE_NAME = "document-count";
    private static final Comparator<TfIdfScore> TF_IDF_SCORE_COMPARATOR =
            Comparator.comparing(TfIdfScore::getTfIdf).thenComparing(TfIdfScore::getTerm);
    @Option(names = "--s3-backed", arity = "0..1")
    private boolean useS3 = false;
    @Option(names = "--as-document", arity = "0..1")
    private boolean asDocument = false;

    public static void main(final String[] args) {
        startApplication(new TfIdfApplication(), args);
    }

    private static TfIdfScore higher(final TfIdfScore score1, final TfIdfScore score2) {
        return TF_IDF_SCORE_COMPARATOR.compare(score1, score2) >= 0 ? score1 : score2;
    }

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        // read input as document or plain text
        final KStream<String, String> texts = this.asDocument ?
                builder.<String, Document>stream(this.getInputTopics()).mapValues(Document::getText) :
                builder.stream(this.getInputTopics());

        // count number of documents (used in idf)
        texts.mapValues((k, v) -> 0)
                .groupBy((k, v) -> ALL, Grouped.with(Serdes.Integer(), Serdes.Integer()))
                .count()
                .toStream()
                .to(this.getDocumentCountTopic(), Produced.with(Serdes.Integer(), Serdes.Long()));

        // register global state store for document count
        final KeyValueBytesStoreSupplier documentCountStoreSupplier =
                Stores.inMemoryKeyValueStore(DOCUMENT_COUNT_STATE_STORE_NAME);
        final StoreBuilder<KeyValueStore<Integer, Long>> documentCountStore =
                Stores.keyValueStoreBuilder(documentCountStoreSupplier, Serdes.Integer(), Serdes.Long());
        builder.addGlobalStore(documentCountStore, this.getDocumentCountTopic(),
                Consumed.with(Serdes.Integer(), Serdes.Long()),
                () -> new GlobalStateStoreUpdater<>(documentCountStore.name()));

        // register state store for document frequencies
        final KeyValueBytesStoreSupplier termOccurrenceStoreSupplier =
                Stores.inMemoryKeyValueStore(TERM_OCCURRENCES_STATE_STORE_NAME);
        final StoreBuilder<KeyValueStore<String, Long>> termOccurrenceStore =
                Stores.keyValueStoreBuilder(termOccurrenceStoreSupplier, null, Serdes.Long());
        builder.addStateStore(termOccurrenceStore);
        final String termOccurrenceStoreName = termOccurrenceStore.name();

        // compute all tfs in a document
        final KStream<String, TermFrequency> termFrequencies =
                texts.flatTransform(() -> new TermFrequencyCalculator(documentCountStore.name()));

        final KStream<String, TfIdfScore> tfIdfs = termFrequencies
                // repartition by term (necessary for state store)
                .through(this.getTermFrequenciesTopic(), Produced.valueSerde(this.createAvroSerde()))
                // compute tf-idf
                .transform(() -> new TfIdfCalculator(termOccurrenceStoreName), termOccurrenceStoreName);

        tfIdfs.to(this.getOutputTopic(), Produced.valueSerde(this.createAvroSerde()));

        tfIdfs.groupByKey(Grouped.with(null, this.createAvroSerde()))
                .reduce(TfIdfApplication::higher)
                .toStream()
                .to(this.getMostImportantTermTopic(), Produced.valueSerde(this.createAvroSerde()));
    }

    @Override
    public String getUniqueAppId() {
        return "tf-idf-" + this.getOutputTopic();
    }

    String getMostImportantTermTopic() {
        return this.getOutputTopic() + MOST_IMPORTANT_TERM_TOPIC_SUFFIX;
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        final Class<?> valueSerde = this.asDocument ? SpecificAvroSerde.class : StringSerde.class;
        if (this.useS3) {
            kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, S3BackedSerde.class);
            kafkaProperties.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, valueSerde);
        } else {
            kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        }
        return kafkaProperties;
    }

    @Override
    protected void runStreamsApplication() {
        try (final TopicClient topicClient = TopicClient
                .create(originals(this.getKafkaProperties()), Duration.ofSeconds(10L))) {
            final TopicSettings settings = topicClient.describe(this.getInputTopic());
            final Map<String, String> config = emptyMap();
            topicClient.createIfNotExists(this.getOutputTopic(), settings, config);
            topicClient.createIfNotExists(this.getTermFrequenciesTopic(), settings, config);
            topicClient.createIfNotExists(this.getDocumentCountTopic(), settings, config);
            topicClient.createIfNotExists(this.getMostImportantTermTopic(), settings, config);
        }
        super.runStreamsApplication();
    }

    String getDocumentCountTopic() {
        return this.getOutputTopic() + DOCUMENT_COUNT_TOPIC_SUFFIX;
    }

    <T extends SpecificRecord> Serde<T> createAvroSerde() {
        final Serde<T> serde = new SpecificAvroSerde<>();
        final Map<String, Object> config = originals(this.getKafkaProperties());
        serde.configure(config, false);
        return serde;
    }

    private String getTermFrequenciesTopic() {
        return this.getOutputTopic() + TERM_FREQUENCIES_TOPIC_SUFFIX;
    }

}
