package com.bakdata.kafka.tfidf;

import com.bakdata.util.seq2.PairSeq;
import com.bakdata.util.seq2.Seq2;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
class TermFrequencyCalculator
        implements Transformer<String, String, Iterable<KeyValue<String, TermFrequency>>> {
    private static final Pattern WHITE_SPACE = Pattern.compile("\\W+");
    private final @NonNull String documentCountStateStoreName;
    private KeyValueStore<Integer, Long> documentCountStore = null;

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
        this.documentCountStore =
                (KeyValueStore<Integer, Long>) context.getStateStore(this.documentCountStateStoreName);
    }

    @Override
    public Iterable<KeyValue<String, TermFrequency>> transform(final String document, final String text) {
        final Map<String, Long> termFrequencies = Seq2.of(WHITE_SPACE.split(text))
                .grouped(Function.identity())
                .mapValues(Stream::count)
                .toMap();
        final long maxFrequency = PairSeq.seq(termFrequencies)
                .values()
                .max()
                .orElse(0L);
        // current document is usually not counted yet
        final long documentCount =
                Optional.ofNullable(this.documentCountStore.get(TfIdfApplication.ALL)).orElse(0L) + 1L;
        return PairSeq.seq(termFrequencies)
                .map((term, v) -> {
                    final double tf = v.doubleValue() / maxFrequency;
                    return new KeyValue<>(term, TermFrequency.newBuilder()
                            .setDocument(document)
                            .setTf(tf)
                            .setDocumentCount(documentCount)
                            .build());
                })
                .toList();
    }

    @Override
    public void close() {
        // do not close the key-value store! https://issues.apache.org/jira/browse/KAFKA-4919
    }
}
