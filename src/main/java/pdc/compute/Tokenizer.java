package pdc.compute;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class Tokenizer {
    private Tokenizer() {
    }

    // tokenizing the input lines
    public static List<String> tokens(String line) {
        if (line == null || line.isBlank()) {
            return List.of();
        }
        return Arrays.stream(line.toLowerCase(Locale.ROOT).split("[^a-zA-Z0-9]+"))
                .filter(token -> !token.isBlank())
                .collect(Collectors.toList());
    }
}
