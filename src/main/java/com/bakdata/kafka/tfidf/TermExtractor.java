package com.bakdata.kafka.tfidf;

import com.bakdata.util.seq2.Seq2;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TermExtractor {
    private static final Pattern WHITE_SPACE = Pattern.compile("\\s+");
    private static final Pattern LEADING_NON_WORD = Pattern.compile("^[^a-zA-Z0-9]+");
    private static final Pattern TRAILING_NON_WORD = Pattern.compile("[^a-zA-Z0-9]+$");

    static Seq2<String> extractTerms(final String text) {
        return Seq2.of(WHITE_SPACE.split(text))
                .map(String::toLowerCase)
                .map(TermExtractor::removeLeadingSpecialCharacters)
                .map(TermExtractor::removeTrailingSpecialCharacters)
                .filterNot(String::isEmpty);
    }

    private static String removeTrailingSpecialCharacters(final String s) {
        return removeAll(s, TRAILING_NON_WORD);
    }

    private static String removeAll(final String s, final Pattern pattern) {
        return pattern.matcher(s).replaceAll("");
    }

    private static String removeLeadingSpecialCharacters(final String s) {
        return removeAll(s, LEADING_NON_WORD);
    }
}
