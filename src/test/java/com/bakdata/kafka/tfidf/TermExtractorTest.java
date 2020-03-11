package com.bakdata.kafka.tfidf;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TermExtractorTest {

    @Test
    void shouldExtractTerms() {
        assertThat(TermExtractor.extractTerms("foo bar\tbaz  qux").toList())
                .hasSize(4)
                .containsExactly("foo", "bar", "baz", "qux");
    }

    @Test
    void shouldRemoveLeadingAndTrailingSpecialCharacters() {
        assertThat(TermExtractor.extractTerms("-foo b-ar baz_").toList())
                .hasSize(3)
                .containsExactly("foo", "b-ar", "baz");
    }

    @Test
    void shouldRemoveSpecialCharacters() {
        assertThat(TermExtractor.extractTerms("foo . bar").toList())
                .hasSize(2)
                .containsExactly("foo", "bar");
    }

}